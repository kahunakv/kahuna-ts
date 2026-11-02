import * as grpc from '@grpc/grpc-js';

import {
  DURABILITY_CODES,
  RANGE_LOCK_MODE_CODES,
  READ_VALIDATION_CODES,
  DECISION_DURABILITY_CODES,
  SET_IF_EQUAL_TO_REVISION_FLAG,
  SET_IF_EQUAL_TO_VALUE_FLAG,
  TRANSACTION_LOCKING_CODES,
  TRANSACTION_CONFLICT_POLICY_CODES,
  TRANSACTION_PRIORITY_CODES,
  HISTORY_HOLD_OPERATION_CODES,
  KEY_VALUE_READ_MODE_CODES,
  WATCH_SCOPE_KIND_CODES,
  commitRefusalFromCode,
  durabilityFromCode,
  isTransientKeyValueCode,
  keyValueResponseFromCode,
  lockResponseFromCode,
  routeProvenanceFromCode,
  sequenceResponseFromCode,
  watchEventKindFromCode,
  watchFrameTypeFromCode,
  watchLagReasonFromCode,
  type Durability,
  type KeyValueResponseCode,
  type RangeLockMode,
  type RoutingDomain,
  type TransactionPriority,
} from '../enums.js';
import {
  KahunaBackupError,
  KahunaError,
  OperationAbortedError,
  commitRefusalError,
  isAbortError,
  throwIfAborted,
} from '../errors.js';
import { HLC_ZERO, type HlcTimestamp } from '../hlc.js';
import type { SecurityOptions } from '../options.js';
import type {
  BackupGcResult,
  BackupInfo,
  BatchOutcome,
  BatchReadResult,
  BatchWriteResult,
  ClusterLeaveResult,
  ClusterMembership,
  ClusterPlacement,
  DeleteManyItem,
  ExtendOutcome,
  GetManyItem,
  HistoryHoldAnswer,
  HistoryHoldRequest,
  LockAcquisition,
  LockInfo,
  MergeRangesResult,
  RangeBounds,
  RangeLockRequest,
  RangeMap,
  RangePage,
  ReadOutcome,
  RegisterKeyRangeResult,
  RemoveKeyRangeResult,
  RestoreResult,
  RouteHint,
  RoutingMetadata,
  ScanItem,
  ScriptParameter,
  ScriptResult,
  SequenceAllocationOutcome,
  SequenceEntry,
  SequenceOutcome,
  SequenceUpdate,
  SetManyItem,
  SetReplicationFactorResult,
  SnapshotFloor,
  SnapshotHold,
  SplitRangeResult,
  TransactionCommit,
  TransactionOptions,
  TransactionStart,
  WatchFrame,
  WatchRequest,
  WriteOutcome,
} from '../types.js';
import { NO_OPERATION_ID, isOperationIdEmpty } from '../types.js';
import { GrpcChannelPool, type ServiceClients } from './grpc-channels.js';
import {
  LazyBackoff,
  MUST_RETRY_ATTEMPTS,
  lockRetryDeadline,
  monotonicNow,
  waitBeforeMustRetry,
} from './retry.js';
import type {
  CallOptions,
  ReadCallOptions,
  RouteSink,
  TransactionalCallOptions,
  Transport,
  WatchOpening,
  WatchStream,
} from './transport.js';
import {
  isTerminalWatchFrame,
  laggedWatchFrame,
  refusedWatchOpening,
  watchOpeningFromAnswer,
} from '../watch.js';

/** The trailer a backup RPC puts its typed outcome in. */
const BACKUP_OUTCOME_TRAILER = 'kahuna-backup-outcome';

type Message = Record<string, unknown>;

/** Names of the cluster roles, indexed by the proto enum value. */
const CLUSTER_MEMBER_ROLES = ['Learner', 'Voter', 'Leaving', 'NotMember'];

/** Names of the replica roles, indexed by the proto enum value. */
const PARTITION_REPLICA_ROLES = ['Voter', 'Learner', 'Removing'];

/** The gRPC metadata entry that carries the admin key. */
export const ADMIN_KEY_METADATA_NAME = 'x-kahuna-admin-key';

/** Names of the leave outcomes, indexed by the proto enum value. */
const LEAVE_OUTCOMES = [
  'Committed',
  'NotAMember',
  'RefusedInsufficientVoters',
  'NotInitialized',
  'NoLeader',
  'Timeout',
  'RefusedDrainInProgress',
  'DrainTimedOut',
  'Evicted',
];

/**
 * Talks to Kahuna over gRPC.
 *
 * Every call is a unary RPC on one of the five services the server exposes, except
 * the range scan, which reads a server stream. The `.proto` files travel with the
 * package and load at run time, so building this client needs no protoc.
 */
export class GrpcTransport implements Transport {
  readonly kind = 'grpc' as const;

  routeSink: RouteSink | null = null;

  private readonly pool: GrpcChannelPool;

  private readonly defaultTimeoutMs: number;

  /** The admin key, sent with the admin calls only; null sends none. */
  private readonly adminApiKey: string | null;

  constructor(
    options: SecurityOptions & { defaultOperationTimeoutMs?: number; adminApiKey?: string | null } = {},
  ) {
    this.pool = new GrpcChannelPool(options);
    this.defaultTimeoutMs = options.defaultOperationTimeoutMs ?? 30_000;
    this.adminApiKey = options.adminApiKey ? options.adminApiKey : null;
  }

  async close(): Promise<void> {
    this.pool.close();
  }

  // ── Plumbing ───────────────────────────────────────────────────────────────

  private services(url: string): ServiceClients {
    return this.pool.get(url);
  }

  private learnRoute(domain: RoutingDomain, resource: string, response: Message, url: string): void {
    this.routeSink?.learn(domain, resource, readRouteHint(response['Route']), url);
  }

  private learnBatchRoutes(
    items: readonly { key: string; routeIndex: number }[],
    routes: unknown,
    url: string,
  ): void {
    this.routeSink?.learnBatch('keyValue', items, readRouteTable(routes), url);
  }

  /** Invokes one unary RPC and reports a transport failure to the route sink. */
  private call<T extends Message>(
    url: string,
    client: grpc.Client,
    method: string,
    request: Message,
    signal: AbortSignal | undefined,
    metadata?: grpc.Metadata,
  ): Promise<T> {
    return invokeUnary<T>(client, method, request, signal, this.defaultTimeoutMs, metadata).catch(
      (error: unknown) => {
        if (!isAbortError(error)) this.routeSink?.reportEndpointFailure(url);
        throw error;
      },
    );
  }

  /**
   * Invokes one admin RPC. It sends the admin key when one is configured. It turns
   * the node's `UNAUTHENTICATED` refusal into an error that names the missing key.
   */
  private async adminCall<T extends Message>(
    url: string,
    client: grpc.Client,
    method: string,
    request: Message,
    signal: AbortSignal | undefined,
  ): Promise<T> {
    let metadata: grpc.Metadata | undefined;
    if (this.adminApiKey !== null) {
      metadata = new grpc.Metadata();
      metadata.set(ADMIN_KEY_METADATA_NAME, this.adminApiKey);
    }

    try {
      return await this.call<T>(url, client, method, request, signal, metadata);
    } catch (error) {
      if ((error as { code?: unknown } | null)?.code === grpc.status.UNAUTHENTICATED) {
        throw KahunaError.lock(
          `The node refused the admin operation ${method}: a valid admin key is required (adminApiKey).`,
          'errored',
          { cause: error },
        );
      }
      throw error;
    }
  }

  /**
   * Runs a key/value RPC and repeats it while the server answers `mustRetry` or
   * `waitingForReplication`.
   */
  private async callWithMustRetry<T extends Message>(
    url: string,
    method: string,
    request: Message,
    signal: AbortSignal | undefined,
    carryForward?: (request: Message, response: T) => void,
  ): Promise<T> {
    const client = this.services(url).keyValuer;

    for (let attempt = 0; ; attempt++) {
      const response = await this.call<T>(url, client, method, request, signal);
      if (!isTransientKeyValueCode(keyValueResponseFromCode(response['Type'] as number))) {
        return response;
      }

      carryForward?.(request, response);

      if (attempt + 1 >= MUST_RETRY_ATTEMPTS) {
        throw KahunaError.keyValue('Retries exhausted.', 'mustRetry');
      }
      await waitBeforeMustRetry(attempt, signal);
    }
  }

  // ── Locks ──────────────────────────────────────────────────────────────────

  async acquireLock(
    url: string,
    resource: string,
    owner: Uint8Array,
    expiryMs: number,
    durability: Durability,
    options?: CallOptions,
  ): Promise<LockAcquisition> {
    const request = {
      Resource: resource,
      Owner: Buffer.from(owner),
      ExpiresMs: expiryMs,
      Durability: DURABILITY_CODES[durability],
    };

    // The caller chose to wait for this lock, so `mustRetry` is unbounded here and
    // termination is its own signal's job.
    const backoff = new LazyBackoff();
    const client = this.services(url).locker;

    for (;;) {
      throwIfAborted(options?.signal);

      const response = await this.call<Message>(url, client, 'TryLock', request, options?.signal);
      this.learnRoute('lock', resource, response, url);

      const type = lockResponseFromCode(response['Type'] as number);
      const fencingToken = numberOf(response['FencingToken']);
      const servedFrom = stringOrNull(response['ServedFrom']);

      if (type === 'locked') return { result: 'acquired', fencingToken, servedFrom };
      if (type === 'busy') return { result: 'conflicted', fencingToken, servedFrom };
      if (type !== 'mustRetry') throw KahunaError.lock('Failed to lock', type);

      await backoff.wait(options?.signal);
    }
  }

  async releaseLock(
    url: string,
    resource: string,
    owner: Uint8Array,
    durability: Durability,
    options?: CallOptions,
  ): Promise<boolean> {
    const request = {
      Resource: resource,
      Owner: Buffer.from(owner),
      Durability: DURABILITY_CODES[durability],
    };

    return this.lockMustRetryLoop(url, 'Unlock', request, resource, options, (type) => {
      if (type === 'unlocked') return { value: true };
      if (type === 'lockDoesNotExist') return { value: false };
      if (type !== 'mustRetry') throw KahunaError.lock(`Failed to unlock: ${type}`, type);
      return null;
    });
  }

  async extendLock(
    url: string,
    resource: string,
    owner: Uint8Array,
    expiryMs: number,
    durability: Durability,
    options?: CallOptions,
  ): Promise<ExtendOutcome> {
    const request = {
      Resource: resource,
      Owner: Buffer.from(owner),
      ExpiresMs: expiryMs,
      Durability: DURABILITY_CODES[durability],
    };

    return this.lockMustRetryLoop(
      url,
      'TryExtendLock',
      request,
      resource,
      options,
      (type, response) => {
        if (type === 'extended') {
          return { value: { extended: true, fencingToken: numberOf(response['FencingToken']) } };
        }
        if (type !== 'mustRetry') throw KahunaError.lock(`Failed to extend lock: ${type}`, type);
        return null;
      },
    );
  }

  async getLock(
    url: string,
    resource: string,
    durability: Durability,
    options?: CallOptions,
  ): Promise<LockInfo | null> {
    // The proto spells this field in lower case, unlike every other request.
    const request = { Resource: resource, durability: DURABILITY_CODES[durability] };

    return this.lockMustRetryLoop(url, 'GetLock', request, resource, options, (type, response) => {
      if (type === 'got') {
        return {
          value: {
            owner: readBytes(response['Owner']),
            expires: readHlc(response, 'Expires'),
            fencingToken: numberOf(response['FencingToken']),
          } satisfies LockInfo,
        };
      }
      if (type !== 'mustRetry') {
        throw KahunaError.lock(`Failed to get lock information: ${type}`, type);
      }
      return null;
    });
  }

  private async lockMustRetryLoop<T>(
    url: string,
    method: string,
    request: Message,
    resource: string,
    options: CallOptions | undefined,
    decide: (
      type: ReturnType<typeof lockResponseFromCode>,
      response: Message,
    ) => { value: T } | null,
  ): Promise<T> {
    const client = this.services(url).locker;
    let retries = 0;
    let deadline = 0;

    for (;;) {
      throwIfAborted(options?.signal);

      const response = await this.call<Message>(url, client, method, request, options?.signal);
      this.learnRoute('lock', resource, response, url);

      const decided = decide(lockResponseFromCode(response['Type'] as number), response);
      if (decided !== null) return decided.value;

      if (retries === 0) deadline = lockRetryDeadline();
      else if (monotonicNow() >= deadline) {
        throw KahunaError.lock('Retries exhausted.', 'mustRetry');
      }

      await waitBeforeMustRetry(retries, options?.signal);
      retries++;
    }
  }

  // ── Key/value point operations ────────────────────────────────────────────

  async set(
    url: string,
    key: string,
    value: Uint8Array | null,
    expiryMs: number,
    flags: number,
    durability: Durability,
    options?: TransactionalCallOptions,
  ): Promise<WriteOutcome> {
    return this.writeKeyValue(url, key, options, {
      ...transactionFields(options),
      Key: key,
      ...optionalBytes('Value', value),
      Flags: flags,
      ExpiresMs: expiryMs,
      Durability: DURABILITY_CODES[durability],
    });
  }

  async compareValueAndSet(
    url: string,
    key: string,
    value: Uint8Array | null,
    compareValue: Uint8Array | null,
    expiryMs: number,
    durability: Durability,
    options?: TransactionalCallOptions,
  ): Promise<WriteOutcome> {
    return this.writeKeyValue(url, key, options, {
      ...transactionFields(options),
      Key: key,
      ...optionalBytes('Value', value),
      ...optionalBytes('CompareValue', compareValue),
      Flags: SET_IF_EQUAL_TO_VALUE_FLAG,
      ExpiresMs: expiryMs,
      Durability: DURABILITY_CODES[durability],
    });
  }

  async compareRevisionAndSet(
    url: string,
    key: string,
    value: Uint8Array | null,
    compareRevision: number,
    expiryMs: number,
    durability: Durability,
    options?: TransactionalCallOptions,
  ): Promise<WriteOutcome> {
    return this.writeKeyValue(url, key, options, {
      ...transactionFields(options),
      Key: key,
      ...optionalBytes('Value', value),
      CompareRevision: compareRevision,
      Flags: SET_IF_EQUAL_TO_REVISION_FLAG,
      ExpiresMs: expiryMs,
      Durability: DURABILITY_CODES[durability],
    });
  }

  private async writeKeyValue(
    url: string,
    key: string,
    options: TransactionalCallOptions | undefined,
    request: Message,
  ): Promise<WriteOutcome> {
    const client = this.services(url).keyValuer;
    let retries = 0;
    let type: KeyValueResponseCode;

    do {
      throwIfAborted(options?.signal);

      const response = await this.call<Message>(
        url,
        client,
        'TrySetKeyValue',
        request,
        options?.signal,
      );
      this.learnRoute('keyValue', key, response, url);

      type = keyValueResponseFromCode(response['Type'] as number);
      const revision = numberOf(response['Revision']);
      const elapsed = numberOf(response['TimeElapsedMs']);

      if (type === 'set') return { success: true, revision, timeElapsedMs: elapsed };
      if (type === 'notSet') return { success: false, revision, timeElapsedMs: elapsed };

      if (++retries >= MUST_RETRY_ATTEMPTS) {
        throw KahunaError.keyValue('Retries exhausted.', 'mustRetry');
      }
      if (isTransientKeyValueCode(type)) await waitBeforeMustRetry(retries - 1, options?.signal);
    } while (isTransientKeyValueCode(type));

    throw KahunaError.keyValue(`Failed to set key/value: ${type}`, type);
  }

  async get(
    url: string,
    key: string,
    revision: number,
    durability: Durability,
    options?: ReadCallOptions,
  ): Promise<ReadOutcome> {
    const request = {
      ...transactionFields(options),
      Key: key,
      Revision: revision,
      Durability: DURABILITY_CODES[durability],
      ...hlcFields('ReadTimestamp', options?.readTimestamp),
      ReadMode: KEY_VALUE_READ_MODE_CODES[options?.readMode ?? 'default'],
    };

    const client = this.services(url).keyValuer;
    let retries = 0;
    let type: KeyValueResponseCode;

    do {
      throwIfAborted(options?.signal);

      const response = await this.call<Message>(
        url,
        client,
        'TryGetKeyValue',
        request,
        options?.signal,
      );
      this.learnRoute('keyValue', key, response, url);

      type = keyValueResponseFromCode(response['Type'] as number);
      const responseRevision = numberOf(response['Revision']);
      const elapsed = numberOf(response['TimeElapsedMs']);

      if (type === 'get') {
        return {
          success: true,
          value: readBytes(response['Value']),
          revision: responseRevision,
          lastModified: readHlc(response, 'LastModified'),
          timeElapsedMs: elapsed,
        };
      }
      if (type === 'doesNotExist') {
        return {
          success: false,
          value: null,
          revision: responseRevision,
          lastModified: HLC_ZERO,
          timeElapsedMs: elapsed,
        };
      }

      if (++retries >= MUST_RETRY_ATTEMPTS) {
        throw KahunaError.keyValue('Retries exhausted.', 'mustRetry');
      }
      if (isTransientKeyValueCode(type)) await waitBeforeMustRetry(retries - 1, options?.signal);
    } while (isTransientKeyValueCode(type));

    throw KahunaError.keyValue(`Failed to get key/value: ${type}`, type);
  }

  async exists(
    url: string,
    key: string,
    revision: number,
    durability: Durability,
    options?: ReadCallOptions,
  ): Promise<WriteOutcome> {
    const request = {
      ...transactionFields(options),
      Key: key,
      Revision: revision,
      Durability: DURABILITY_CODES[durability],
      ...hlcFields('ReadTimestamp', options?.readTimestamp),
    };

    return this.pointOutcome(url, key, 'TryExistsKeyValue', request, options, 'exists', (type) =>
      KahunaError.keyValue(`Failed to check if exists key/value: ${type}`, type),
    );
  }

  async delete(
    url: string,
    key: string,
    durability: Durability,
    options?: TransactionalCallOptions,
  ): Promise<WriteOutcome> {
    const request = {
      ...transactionFields(options),
      Key: key,
      Durability: DURABILITY_CODES[durability],
    };

    return this.pointOutcome(url, key, 'TryDeleteKeyValue', request, options, 'deleted', (type) =>
      KahunaError.keyValue(`Failed to delete key/value: ${type}`, type),
    );
  }

  async extend(
    url: string,
    key: string,
    expiryMs: number,
    durability: Durability,
    options?: TransactionalCallOptions,
  ): Promise<WriteOutcome> {
    const request = {
      ...transactionFields(options),
      Key: key,
      ExpiresMs: expiryMs,
      Durability: DURABILITY_CODES[durability],
    };

    return this.pointOutcome(url, key, 'TryExtendKeyValue', request, options, 'extended', (type) =>
      KahunaError.keyValue(`Failed to extend key/value: ${type}`, type),
    );
  }

  private async pointOutcome(
    url: string,
    key: string,
    method: string,
    request: Message,
    options: CallOptions | undefined,
    successCode: KeyValueResponseCode,
    failure: (type: KeyValueResponseCode) => KahunaError,
  ): Promise<WriteOutcome> {
    const client = this.services(url).keyValuer;
    let retries = 0;
    let type: KeyValueResponseCode;

    do {
      throwIfAborted(options?.signal);

      const response = await this.call<Message>(url, client, method, request, options?.signal);
      this.learnRoute('keyValue', key, response, url);

      type = keyValueResponseFromCode(response['Type'] as number);
      const revision = numberOf(response['Revision']);
      const elapsed = numberOf(response['TimeElapsedMs']);

      if (type === successCode) return { success: true, revision, timeElapsedMs: elapsed };
      if (type === 'doesNotExist') return { success: false, revision, timeElapsedMs: elapsed };

      if (++retries >= MUST_RETRY_ATTEMPTS) {
        throw KahunaError.keyValue('Retries exhausted.', 'mustRetry');
      }
      if (isTransientKeyValueCode(type)) await waitBeforeMustRetry(retries - 1, options?.signal);
    } while (isTransientKeyValueCode(type));

    throw failure(type);
  }

  // ── Key/value batches ──────────────────────────────────────────────────────

  async setMany(
    url: string,
    items: readonly SetManyItem[],
    options?: CallOptions,
  ): Promise<BatchOutcome<BatchWriteResult>> {
    throwIfAborted(options?.signal);

    const request = {
      Items: items.map((item) => ({
        ...hlcFields('TransactionId', HLC_ZERO),
        Key: item.key,
        ...optionalBytes('Value', item.value === undefined ? null : normalizeValue(item.value)),
        ...optionalBytes(
          'CompareValue',
          item.compareValue === undefined ? null : normalizeValue(item.compareValue),
        ),
        CompareRevision: item.compareRevision ?? 0,
        Flags: item.flags ?? 1,
        ExpiresMs: item.expiry ?? 0,
        Durability: DURABILITY_CODES[item.durability ?? 'persistent'],
      })),
    };

    const response = await this.call<Message>(
      url,
      this.services(url).keyValuer,
      'TrySetManyKeyValue',
      request,
      options?.signal,
    );

    const parsed = readBatchWriteItems(response['Items']);
    this.learnBatchRoutes(parsed, response['Routes'], url);
    return { items: parsed, timeElapsedMs: numberOf(response['TimeElapsedMs']) };
  }

  async deleteMany(
    url: string,
    items: readonly DeleteManyItem[],
    options?: TransactionalCallOptions,
  ): Promise<BatchOutcome<BatchWriteResult>> {
    throwIfAborted(options?.signal);

    const transaction = options?.transaction;
    const request: Message = {
      Items: items.map((item) => ({
        ...hlcFields('TransactionId', transaction?.transactionId ?? HLC_ZERO),
        Key: item.key,
        Durability: DURABILITY_CODES[item.durability ?? 'persistent'],
      })),
    };

    if (transaction && transaction.coordinatorKey && !isOperationIdEmpty(transaction.operationId)) {
      request['CoordinatorKey'] = transaction.coordinatorKey;
      request['OperationIdHigh'] = transaction.operationId.high;
      request['OperationIdLow'] = transaction.operationId.low;
    }

    const response = await this.call<Message>(
      url,
      this.services(url).keyValuer,
      'TryDeleteManyKeyValue',
      request,
      options?.signal,
    );

    const parsed = readBatchWriteItems(response['Items']);
    this.learnBatchRoutes(parsed, response['Routes'], url);
    return { items: parsed, timeElapsedMs: numberOf(response['TimeElapsedMs']) };
  }

  async getMany(
    url: string,
    items: readonly GetManyItem[],
    options?: ReadCallOptions,
  ): Promise<BatchOutcome<BatchReadResult>> {
    return this.readManyKeyValues(url, 'TryGetManyValues', items, options);
  }

  async existsMany(
    url: string,
    items: readonly GetManyItem[],
    options?: ReadCallOptions,
  ): Promise<BatchOutcome<BatchReadResult>> {
    return this.readManyKeyValues(url, 'TryExistsManyValues', items, options);
  }

  private async readManyKeyValues(
    url: string,
    method: string,
    items: readonly GetManyItem[],
    options: ReadCallOptions | undefined,
  ): Promise<BatchOutcome<BatchReadResult>> {
    throwIfAborted(options?.signal);

    const request = {
      ...hlcFields('TransactionId', options?.transaction?.transactionId ?? HLC_ZERO),
      ...hlcFields('ReadTimestamp', options?.readTimestamp),
      Items: items.map((item) => ({
        Key: item.key,
        Revision: item.revision ?? -1,
        Durability: DURABILITY_CODES[item.durability ?? 'persistent'],
      })),
    };

    const response = await this.call<Message>(
      url,
      this.services(url).keyValuer,
      method,
      request,
      options?.signal,
    );

    const parsed: BatchReadResult[] = asArray(response['Items']).map((row) => ({
      key: stringOf(row['Key']),
      type: keyValueResponseFromCode(row['Type'] as number),
      value: readBytes(row['Value']),
      revision: numberOf(row['Revision']),
      lastModified: readHlc(row, 'LastModified'),
      durability: durabilityFromCode(row['Durability'] as number),
      routeIndex: numberOf(row['RouteIndex']),
    }));

    this.learnBatchRoutes(parsed, response['Routes'], url);
    return { items: parsed, timeElapsedMs: 0 };
  }

  // ── Scans ──────────────────────────────────────────────────────────────────

  async getByBucket(
    url: string,
    prefixKey: string,
    durability: Durability,
    options?: ReadCallOptions,
  ): Promise<ScanItem[]> {
    const response = await this.callWithMustRetry<Message>(
      url,
      'GetByBucket',
      {
        ...transactionFields(options),
        PrefixKey: prefixKey,
        Durability: DURABILITY_CODES[durability],
        ...hlcFields('ReadTimestamp', options?.readTimestamp),
      },
      options?.signal,
    );

    return readBucketItems(response, prefixKey);
  }

  async scanAllByPrefix(
    url: string,
    prefixKey: string,
    durability: Durability,
    options?: ReadCallOptions,
  ): Promise<ScanItem[]> {
    const response = await this.callWithMustRetry<Message>(
      url,
      'ScanAllByPrefix',
      {
        PrefixKey: prefixKey,
        Durability: DURABILITY_CODES[durability],
        ...hlcFields('ReadTimestamp', options?.readTimestamp),
      },
      options?.signal,
    );

    return readBucketItems(response, prefixKey);
  }

  async getByRange(
    url: string,
    bounds: RangeBounds,
    limit: number,
    durability: Durability,
    options?: ReadCallOptions,
  ): Promise<RangePage> {
    const response = await this.callWithMustRetry<Message>(
      url,
      'GetByRange',
      {
        ...transactionFields(options),
        ...rangeFields(bounds),
        Limit: limit,
        ...hlcFields('ReadTimestamp', options?.readTimestamp),
        Durability: DURABILITY_CODES[durability],
        ReadMode: KEY_VALUE_READ_MODE_CODES[options?.readMode ?? 'default'],
      },
      options?.signal,
    );

    const type = keyValueResponseFromCode(response['Type'] as number);
    if (type !== 'get' && type !== 'doesNotExist') {
      throw KahunaError.keyValue(`Failed to get by range for '${bounds.prefix}': ${type}.`, type);
    }

    return {
      items: readScanItems(response['Items']),
      nextCursor: stringOrNull(response['NextCursor']),
      hasMore: response['HasMore'] === true,
    };
  }

  async *scanByRange(
    url: string,
    bounds: RangeBounds,
    pageSize: number,
    durability: Durability,
    options?: ReadCallOptions,
  ): AsyncIterable<ScanItem> {
    const request = {
      ...transactionFields(options),
      ...rangeFields(bounds),
      Limit: pageSize,
      ...hlcFields('ReadTimestamp', options?.readTimestamp),
      Durability: DURABILITY_CODES[durability],
      ReadMode: KEY_VALUE_READ_MODE_CODES[options?.readMode ?? 'default'],
    };

    const stream = invokeServerStream<Message>(
      this.services(url).keyValuer,
      'GetByRangeStream',
      request,
      options?.signal,
    );

    for await (const page of stream) {
      const type = keyValueResponseFromCode(page['Type'] as number);
      if (type !== 'get' && type !== 'doesNotExist') {
        throw KahunaError.keyValue(
          `Failed to scan by range for '${bounds.prefix}': ${type}.`,
          type,
        );
      }
      yield* readScanItems(page['Items']);
    }
  }

  /**
   * A watch is a long-lived bidirectional call on the shared channel. The call can
   * carry many watches; this client opens one per call, so a dropped call ends
   * exactly one watch.
   */
  async openWatch(url: string, request: WatchRequest, options?: CallOptions): Promise<WatchOpening> {
    return GrpcWatchStream.open(this.services(url).keyValuer, request, options?.signal);
  }

  // ── Transaction locks ──────────────────────────────────────────────────────

  async acquireExclusiveLock(
    url: string,
    key: string,
    expiryMs: number,
    durability: Durability,
    options: TransactionalCallOptions,
  ): Promise<boolean> {
    const response = await this.callWithMustRetry<Message>(
      url,
      'TryAcquireExclusiveLock',
      {
        ...transactionFields(options),
        Key: key,
        ExpiresMs: expiryMs,
        Durability: DURABILITY_CODES[durability],
      },
      options.signal,
    );

    const type = keyValueResponseFromCode(response['Type'] as number);
    if (type === 'locked') return true;
    throw KahunaError.keyValue(`Failed to acquire key/value lock for '${key}': ${type}.`, type);
  }

  async acquireExclusivePrefixLock(
    url: string,
    prefixKey: string,
    expiryMs: number,
    durability: Durability,
    options: TransactionalCallOptions,
  ): Promise<boolean> {
    const response = await this.callWithMustRetry<Message>(
      url,
      'TryAcquireExclusivePrefixLock',
      {
        ...transactionFields(options),
        PrefixKey: prefixKey,
        ExpiresMs: expiryMs,
        Durability: DURABILITY_CODES[durability],
      },
      options.signal,
    );

    const type = keyValueResponseFromCode(response['Type'] as number);
    if (type === 'locked') return true;
    throw KahunaError.keyValue(
      `Failed to acquire exclusive prefix lock for '${prefixKey}': ${type}.`,
      type,
    );
  }

  async releaseExclusivePrefixLock(
    url: string,
    prefixKey: string,
    durability: Durability,
    options: TransactionalCallOptions,
  ): Promise<void> {
    await this.call<Message>(
      url,
      this.services(url).keyValuer,
      'TryReleaseExclusivePrefixLock',
      {
        ...hlcFields('TransactionId', options.transaction?.transactionId ?? HLC_ZERO),
        PrefixKey: prefixKey,
        Durability: DURABILITY_CODES[durability],
      },
      options.signal,
    );
  }

  async acquireRangeLock(
    url: string,
    request: RangeLockRequest,
    expiryMs: number,
    durability: Durability,
    mode: RangeLockMode,
    options: TransactionalCallOptions,
  ): Promise<boolean> {
    const response = await this.callWithMustRetry<Message>(
      url,
      'TryAcquireExclusiveRangeLock',
      {
        ...transactionFields(options),
        ...rangeFields(request),
        ExpiresMs: expiryMs,
        Durability: DURABILITY_CODES[durability],
        Mode: RANGE_LOCK_MODE_CODES[mode],
      },
      options.signal,
    );

    const type = keyValueResponseFromCode(response['Type'] as number);
    if (type === 'locked') return true;
    throw KahunaError.keyValue(
      `Failed to acquire range lock for '${request.prefix}': ${type}.`,
      type,
    );
  }

  async releaseRangeLock(
    url: string,
    bounds: RangeBounds,
    durability: Durability,
    options: TransactionalCallOptions,
  ): Promise<void> {
    await this.call<Message>(
      url,
      this.services(url).keyValuer,
      'TryReleaseExclusiveRangeLock',
      {
        ...hlcFields('TransactionId', options.transaction?.transactionId ?? HLC_ZERO),
        ...rangeFields(bounds),
        Durability: DURABILITY_CODES[durability],
      },
      options.signal,
    );
  }

  // ── Transactions ───────────────────────────────────────────────────────────

  async executeScript(
    url: string,
    script: Uint8Array,
    hash: string | null,
    parameters: readonly ScriptParameter[] | null,
    priority: TransactionPriority,
    options?: CallOptions,
  ): Promise<ScriptResult> {
    const request: Message = {
      Script: Buffer.from(script),
      Parameters: (parameters ?? []).map((p) => ({
        Key: p.key,
        ...(p.value === null ? {} : { Value: p.value }),
      })),
      // The proto reserves 0 for "unspecified", so every named priority is one
      // higher there than in the REST contract.
      Priority: TRANSACTION_PRIORITY_CODES[priority] + 1,
    };
    if (hash !== null) request['Hash'] = hash;

    const client = this.services(url).keyValuer;
    // A script's `mustRetry` is a transient the server already classified, and a re-run
    // is a new transaction. None of those clear within an immediate re-issue, so the
    // attempts are spaced like a transaction-session call's.
    const backoff = new LazyBackoff();
    let retries = 0;
    let type: KeyValueResponseCode;
    let reason: string | null = null;

    do {
      throwIfAborted(options?.signal);

      const response = await this.call<Message>(
        url,
        client,
        'TryExecuteTransactionScript',
        request,
        options?.signal,
      );

      const code = numberOf(response['Type']);
      type = keyValueResponseFromCode(code);
      reason = stringOrNull(response['Reason']);

      if (code < 99 || type === 'doesNotExist') {
        return {
          type,
          servedFrom: stringOrNull(response['ServedFrom']),
          values: asArray(response['Values']).map((item) => ({
            key: stringOf(item['Key']),
            value: readBytes(item['Value']),
            revision: numberOf(item['Revision']),
            expires: readHlc(item, 'Expires'),
            lastModified: readHlc(item, 'LastModified'),
          })),
          timeElapsedMs: numberOf(response['TimeElapsedMs']),
        };
      }

      if (type !== 'mustRetry') break;

      if (++retries >= MUST_RETRY_ATTEMPTS) {
        throw KahunaError.keyValue('Retries exhausted.', 'mustRetry');
      }
      await backoff.wait(options?.signal);
    } while (type === 'mustRetry');

    if (reason) throw KahunaError.keyValue(reason, type);
    if (type === 'aborted') throw KahunaError.keyValue('Transaction aborted', type);
    throw KahunaError.keyValue(`Failed to execute key/value transaction:${type}`, type);
  }

  async startTransaction(
    url: string,
    coordinatorKey: string,
    options: TransactionOptions,
    call?: CallOptions,
  ): Promise<TransactionStart> {
    const response = await this.callWithMustRetry<Message>(
      url,
      'StartTransaction',
      {
        CoordinatorKey: coordinatorKey,
        LockingType: TRANSACTION_LOCKING_CODES[options.locking ?? 'pessimistic'],
        Timeout: options.timeout ?? 0,
        AsyncRelease: options.asyncRelease ?? false,
        AutoCommit: options.autoCommit ?? true,
        ReadValidation: READ_VALIDATION_CODES[options.readValidation ?? 'none'],
        DecisionDurability: DECISION_DURABILITY_CODES[options.decisionDurability ?? 'bestEffort'],
        ...hlcFields('ReadTimestamp', options.readTimestamp),
        Priority: TRANSACTION_PRIORITY_CODES[options.priority ?? 'normal'] + 1,
        AdmissionWaitMs: options.admissionWaitMs ?? 0,
        ConflictPolicy: TRANSACTION_CONFLICT_POLICY_CODES[options.conflictPolicy ?? 'normal'] + 1,
        ...(options.origin === undefined ? {} : { Origin: options.origin }),
      },
      call?.signal,
    );

    const type = keyValueResponseFromCode(response['Type'] as number);
    if (type === 'set') return { url, transactionId: readHlc(response, 'TransactionId') };
    throw KahunaError.keyValue(`Failed to start key/value transaction: ${type}`, type);
  }

  async commitTransaction(
    url: string,
    coordinatorKey: string,
    transactionId: HlcTimestamp,
    recordAnchorKey: string | null,
    call?: CallOptions,
  ): Promise<TransactionCommit> {
    const request: Message = {
      CoordinatorKey: coordinatorKey,
      ...hlcFields('TransactionId', transactionId),
    };
    if (recordAnchorKey !== null) request['RecordAnchorKey'] = recordAnchorKey;

    const response = await this.callWithMustRetry<Message>(
      url,
      'CommitTransaction',
      request,
      call?.signal,
      (retryRequest, retryResponse) => {
        const anchor = stringOrNull(retryResponse['RecordAnchorKey']);
        if (anchor !== null) retryRequest['RecordAnchorKey'] = anchor;
      },
    );

    const type = keyValueResponseFromCode(response['Type'] as number);
    if (type === 'committed') {
      return {
        committed: true,
        recordAnchorKey: stringOrNull(response['RecordAnchorKey']),
        commitTimestamp: readHlc(response, 'CommitTimestamp'),
      };
    }
    // The answer names the arm that refused the commit, so a caller can tell a
    // conflict from a lost lock or a deadline without reading server logs.
    throw commitRefusalError(
      type,
      commitRefusalFromCode(response['Refusal'] as number),
      stringOrNull(response['Reason']),
    );
  }

  async rollbackTransaction(
    url: string,
    coordinatorKey: string,
    transactionId: HlcTimestamp,
    recordAnchorKey: string | null,
    call?: CallOptions,
  ): Promise<boolean> {
    const request: Message = {
      CoordinatorKey: coordinatorKey,
      ...hlcFields('TransactionId', transactionId),
    };
    if (recordAnchorKey !== null) request['RecordAnchorKey'] = recordAnchorKey;

    const response = await this.callWithMustRetry<Message>(
      url,
      'RollbackTransaction',
      request,
      call?.signal,
    );

    const type = keyValueResponseFromCode(response['Type'] as number);
    if (type === 'rolledBack') return true;
    throw KahunaError.keyValue(`Failed to rollback key/value transaction: ${type}`, type);
  }

  // ── Sequences ──────────────────────────────────────────────────────────────

  async createSequence(
    url: string,
    name: string,
    initialValue: number,
    increment: number,
    maxValue: number | null,
    blockSize: number | null,
    options?: CallOptions,
  ): Promise<SequenceOutcome> {
    const request: Message = {
      Name: name,
      InitialValue: initialValue,
      Increment: increment,
      Durability: 1,
    };
    if (maxValue !== null) request['MaxValue'] = maxValue;
    if (blockSize !== null) request['BlockSize'] = blockSize;

    return this.sequenceCall(url, 'CreateSequence', name, request, options);
  }

  async updateSequence(
    url: string,
    name: string,
    update: SequenceUpdate,
    options?: CallOptions,
  ): Promise<SequenceOutcome> {
    // The optional fields carry presence on the wire: a field left out of the
    // request leaves that parameter of the sequence as it is.
    const request: Message = {
      Name: name,
      RemoveMaxValue: update.removeMaxValue === true,
      RemoveBlockSize: update.removeBlockSize === true,
      Durability: 1,
    };
    if (update.currentValue !== undefined) request['CurrentValue'] = update.currentValue;
    if (update.increment !== undefined) request['Increment'] = update.increment;
    if (update.initialValue !== undefined) request['InitialValue'] = update.initialValue;
    if (update.maxValue !== undefined) request['MaxValue'] = update.maxValue;
    if (update.blockSize !== undefined) request['BlockSize'] = update.blockSize;

    return this.sequenceCall(url, 'UpdateSequence', name, request, options);
  }

  async getSequence(url: string, name: string, options?: CallOptions): Promise<SequenceOutcome> {
    return this.sequenceCall(url, 'GetSequence', name, { Name: name, Durability: 1 }, options);
  }

  async deleteSequence(url: string, name: string, options?: CallOptions): Promise<SequenceOutcome> {
    return this.sequenceCall(url, 'DeleteSequence', name, { Name: name, Durability: 1 }, options);
  }

  async nextSequenceValue(
    url: string,
    name: string,
    idempotencyKey: string | null,
    options?: CallOptions,
  ): Promise<SequenceAllocationOutcome> {
    const request: Message = { Name: name, Durability: 1 };
    if (idempotencyKey !== null) request['IdempotencyKey'] = idempotencyKey;

    return this.sequenceAllocationCall(url, 'NextSequenceValue', name, request, options);
  }

  async reserveSequenceRange(
    url: string,
    name: string,
    count: number,
    idempotencyKey: string | null,
    options?: CallOptions,
  ): Promise<SequenceAllocationOutcome> {
    const request: Message = { Name: name, Count: count, Durability: 1 };
    if (idempotencyKey !== null) request['IdempotencyKey'] = idempotencyKey;

    return this.sequenceAllocationCall(url, 'ReserveSequenceRange', name, request, options);
  }

  private async sequenceCall(
    url: string,
    method: string,
    name: string,
    request: Message,
    options: CallOptions | undefined,
  ): Promise<SequenceOutcome> {
    const response = await this.call<Message>(
      url,
      this.services(url).sequencer,
      method,
      request,
      options?.signal,
    );
    this.learnRoute('sequence', name, response, url);

    return {
      type: sequenceResponseFromCode(response['Type'] as number),
      entry: readSequenceEntry(response['Sequence']),
      revision: numberOf(response['Revision']),
      timeElapsedMs: numberOf(response['TimeElapsedMs']),
    };
  }

  private async sequenceAllocationCall(
    url: string,
    method: string,
    name: string,
    request: Message,
    options: CallOptions | undefined,
  ): Promise<SequenceAllocationOutcome> {
    const response = await this.call<Message>(
      url,
      this.services(url).sequencer,
      method,
      request,
      options?.signal,
    );
    this.learnRoute('sequence', name, response, url);

    const allocation = (response['Allocation'] as Message | undefined) ?? {};
    return {
      type: sequenceResponseFromCode(response['Type'] as number),
      allocation: {
        name: stringOf(allocation['Name']),
        start: numberOf(allocation['Start']),
        end: numberOf(allocation['End']),
        count: numberOf(allocation['Count']),
        revision: numberOf(allocation['Revision']),
      },
      timeElapsedMs: numberOf(response['TimeElapsedMs']),
    };
  }

  // ── Key ranges ─────────────────────────────────────────────────────────────

  async registerKeyRange(
    url: string,
    keySpace: string,
    options?: CallOptions,
  ): Promise<RegisterKeyRangeResult> {
    const response = await this.adminCall<Message>(
      url,
      this.services(url).keyValuer,
      'RegisterKeyRange',
      { KeySpace: keySpace },
      options?.signal,
    );

    return {
      success: response['Success'] === true,
      status: stringOf(response['Status']),
      seeded: response['Seeded'] === true,
      routingMode: stringOf(response['RoutingMode']),
      descriptorCount: numberOf(response['DescriptorCount']),
      reason: stringOrNull(response['Reason']),
    };
  }

  async removeKeyRange(
    url: string,
    keySpace: string,
    options?: CallOptions,
  ): Promise<RemoveKeyRangeResult> {
    const response = await this.adminCall<Message>(
      url,
      this.services(url).keyValuer,
      'RemoveKeyRange',
      { KeySpace: keySpace },
      options?.signal,
    );

    return {
      success: response['Success'] === true,
      status: stringOf(response['Status']),
      routingMode: stringOf(response['RoutingMode']),
      descriptorCount: numberOf(response['DescriptorCount']),
      reason: stringOrNull(response['Reason']),
    };
  }

  async getRanges(url: string, keySpace: string | null, options?: CallOptions): Promise<RangeMap> {
    const response = await this.call<Message>(
      url,
      this.services(url).cluster,
      'GetRanges',
      { KeySpace: keySpace ?? '' },
      options?.signal,
    );

    return {
      initialized: response['Initialized'] === true,
      localEndpoint: stringOf(response['LocalEndpoint']),
      keySpaces: asArray(response['KeySpaces']).map((space) => ({
        keySpace: stringOf(space['KeySpace']),
        routingMode: stringOf(space['RoutingMode']),
        descriptors: asArray(space['Descriptors']).map((descriptor) => ({
          startKey: stringOrNull(descriptor['StartKey']),
          endKey: stringOrNull(descriptor['EndKey']),
          partitionId: numberOf(descriptor['PartitionId']),
          generation: numberOf(descriptor['Generation']),
        })),
      })),
    };
  }

  async splitRange(
    url: string,
    keySpace: string,
    splitKey: string,
    options?: CallOptions,
  ): Promise<SplitRangeResult> {
    const response = await this.adminCall<Message>(
      url,
      this.services(url).cluster,
      'SplitRange',
      { KeySpace: keySpace, SplitKey: splitKey },
      options?.signal,
    );

    return {
      success: response['Success'] === true,
      status: stringOf(response['Status']),
      determinate: response['Determinate'] === true,
      newPartitionId: numberOf(response['NewPartitionId']),
      newGeneration: numberOf(response['NewGeneration']),
      leaderHint: stringOrNull(response['LeaderHint']),
      reason: stringOrNull(response['Reason']),
    };
  }

  async mergeRanges(url: string, options?: CallOptions): Promise<MergeRangesResult> {
    const response = await this.adminCall<Message>(
      url,
      this.services(url).cluster,
      'MergeRanges',
      {},
      options?.signal,
    );

    return {
      success: response['Success'] === true,
      status: stringOf(response['Status']),
      determinate: response['Determinate'] === true,
      merges: numberOf(response['Merges']),
      leaderHint: stringOrNull(response['LeaderHint']),
      reason: stringOrNull(response['Reason']),
    };
  }

  // ── Cluster ────────────────────────────────────────────────────────────────

  async getRoutingMetadata(
    url: string,
    keySpace: string | null,
    options?: CallOptions,
  ): Promise<RoutingMetadata> {
    const response = await this.call<Message>(
      url,
      this.services(url).cluster,
      'GetRoutingMetadata',
      { KeySpace: keySpace ?? '' },
      options?.signal,
    );

    return {
      initialized: response['Initialized'] === true,
      schemaVersion: numberOf(response['SchemaVersion']),
      hashAlgorithm: stringOf(response['HashAlgorithm']),
      prefixSeparator: stringOf(response['PrefixSeparator']),
      groupSeparator: stringOf(response['GroupSeparator']),
      hashPoolSize: numberOf(response['HashPoolSize']),
      hashPartitionOffset: numberOf(response['HashPartitionOffset']),
      sequenceStorageKeyFormat: stringOf(response['SequenceStorageKeyFormat']),
      reservedKeyPrefix: stringOf(response['ReservedKeyPrefix']),
      localEndpoint: stringOf(response['LocalEndpoint']),
      snapshotVersion: numberOf(response['SnapshotVersion']),
      coherent: response['Coherent'] === true,
      keySpaces: asArray(response['KeySpaces']).map((space) => ({
        keySpace: stringOf(space['KeySpace']),
        routingMode: stringOf(space['RoutingMode']),
        ranges: asArray(space['Ranges']).map((range) => ({
          startKey: stringOrNull(range['StartKey']),
          endKey: stringOrNull(range['EndKey']),
          partitionId: numberOf(range['PartitionId']),
          generation: numberOf(range['Generation']),
        })),
      })),
      leaders: asArray(response['Leaders']).map((leader) => ({
        partitionId: numberOf(leader['PartitionId']),
        endpoint: stringOf(leader['Endpoint']),
      })),
    };
  }

  async getClusterMembership(url: string, options?: CallOptions): Promise<ClusterMembership> {
    const response = await this.call<Message>(
      url,
      this.services(url).cluster,
      'GetMembership',
      {},
      options?.signal,
    );

    return {
      membershipVersion: numberOf(response['MembershipVersion']),
      members: asArray(response['Members']).map((member) => ({
        endpoint: stringOf(member['Endpoint']),
        nodeId: numberOf(member['NodeId']),
        role: nameOf(CLUSTER_MEMBER_ROLES, member['Role']),
        joinedVersion: numberOf(member['JoinedVersion']),
      })),
      localRole: nameOf(CLUSTER_MEMBER_ROLES, response['LocalRole']),
      initialized: response['Initialized'] === true,
    };
  }

  async getClusterPlacement(url: string, options?: CallOptions): Promise<ClusterPlacement> {
    const response = await this.call<Message>(
      url,
      this.services(url).cluster,
      'GetPlacement',
      {},
      options?.signal,
    );

    return {
      replicationFactor: numberOf(response['ReplicationFactor']),
      rebalancerEnabled: response['RebalancerEnabled'] === true,
      initialized: response['Initialized'] === true,
      localEndpoint: stringOf(response['LocalEndpoint']),
      hostedPartitionCount: numberOf(response['HostedPartitionCount']),
      partitions: asArray(response['Partitions']).map((partition) => ({
        partitionId: numberOf(partition['PartitionId']),
        state: stringOf(partition['State']),
        generation: numberOf(partition['Generation']),
        effectiveReplicationFactor: numberOf(partition['EffectiveReplicationFactor']),
        hostedLocally: partition['HostedLocally'] === true,
        replicas: asArray(partition['Replicas']).map((replica) => ({
          endpoint: stringOf(replica['Endpoint']),
          role: nameOf(PARTITION_REPLICA_ROLES, replica['Role']),
        })),
      })),
    };
  }

  async leaveCluster(url: string, options?: CallOptions): Promise<ClusterLeaveResult> {
    const response = await this.adminCall<Message>(
      url,
      this.services(url).cluster,
      'Leave',
      {},
      options?.signal,
    );

    return {
      left: response['Left'] === true,
      drained: response['Drained'] === true,
      outcome: nameOf(LEAVE_OUTCOMES, response['Outcome']),
      membershipVersion: numberOf(response['MembershipVersion']),
      retryable: response['Retryable'] === true,
      reason: stringOf(response['Reason']),
    };
  }

  async setReplicationFactor(
    url: string,
    partitionId: number,
    replicationFactor: number,
    options?: CallOptions,
  ): Promise<SetReplicationFactorResult> {
    const response = await this.adminCall<Message>(
      url,
      this.services(url).cluster,
      'SetReplicationFactor',
      { PartitionId: partitionId, ReplicationFactor: replicationFactor },
      options?.signal,
    );

    return {
      success: response['Success'] === true,
      status: stringOf(response['Status']),
      generation: numberOf(response['Generation']),
      reason: stringOrNull(response['Reason']),
    };
  }

  // ── Snapshot holds ─────────────────────────────────────────────────────────

  async acquireSnapshotHold(
    url: string,
    holderId: string,
    timestamp: HlcTimestamp,
    leaseMs: number,
    options?: CallOptions,
  ): Promise<SnapshotHold> {
    const response = await this.call<Message>(
      url,
      this.services(url).keyValuer,
      'AcquireSnapshotHold',
      { HolderId: holderId, ...hlcFields('Timestamp', timestamp), LeaseMs: leaseMs },
      options?.signal,
    );

    return {
      type: keyValueResponseFromCode(response['Type'] as number),
      holdId: stringOf(response['HoldId']),
      leaseExpiry: readHlc(response, 'LeaseExpiry'),
    };
  }

  async renewSnapshotHold(
    url: string,
    holdId: string,
    leaseMs: number,
    options?: CallOptions,
  ): Promise<Omit<SnapshotHold, 'holdId'>> {
    const response = await this.call<Message>(
      url,
      this.services(url).keyValuer,
      'RenewSnapshotHold',
      { HoldId: holdId, LeaseMs: leaseMs },
      options?.signal,
    );

    return {
      type: keyValueResponseFromCode(response['Type'] as number),
      leaseExpiry: readHlc(response, 'LeaseExpiry'),
    };
  }

  async releaseSnapshotHold(
    url: string,
    holdId: string,
    options?: CallOptions,
  ): Promise<KeyValueResponseCode> {
    const response = await this.call<Message>(
      url,
      this.services(url).keyValuer,
      'ReleaseSnapshotHold',
      { HoldId: holdId },
      options?.signal,
    );

    return keyValueResponseFromCode(response['Type'] as number);
  }

  async getSnapshotFloor(url: string, options?: CallOptions): Promise<SnapshotFloor> {
    const response = await this.call<Message>(
      url,
      this.services(url).keyValuer,
      'GetSnapshotFloor',
      {},
      options?.signal,
    );

    const type = keyValueResponseFromCode(response['Type'] as number);
    if (type !== 'get' && type !== 'set') {
      throw KahunaError.keyValue('GetSnapshotFloor failed', type);
    }

    return {
      effectiveFloor: readHlc(response, 'EffectiveFloor'),
      liveHolds: numberOf(response['LiveHolds']),
    };
  }

  // ── History holds ──────────────────────────────────────────────────────────

  async historyHold(
    url: string,
    request: HistoryHoldRequest,
    options?: CallOptions,
  ): Promise<HistoryHoldAnswer> {
    const message: Message = {
      Operation: HISTORY_HOLD_OPERATION_CODES[request.operation],
      Partitions: [...(request.partitions ?? [])],
      ...hlcFields('Timestamp', request.timestamp),
      LeaseMs: request.leaseMs ?? 0,
      HoldIds: [...(request.holdIds ?? [])],
    };
    if (request.holderId !== null && request.holderId !== undefined) {
      message['HolderId'] = request.holderId;
    }

    const response = await this.call<Message>(
      url,
      this.services(url).keyValuer,
      'HistoryHold',
      message,
      options?.signal,
    );

    return {
      type: keyValueResponseFromCode(response['Type'] as number),
      holds: asArray(response['Holds']).map((hold) => ({
        holdId: stringOf(hold['HoldId']),
        holderId: stringOf(hold['HolderId']),
        partitionId: numberOf(hold['PartitionId']),
        timestamp: readHlc(hold, 'Timestamp'),
        leaseExpiry: readHlc(hold, 'LeaseExpiry'),
      })),
    };
  }

  // ── Backups ────────────────────────────────────────────────────────────────

  async takeFullBackup(url: string, options?: CallOptions): Promise<BackupInfo> {
    return readBackupInfo(await this.backupCall(url, 'TakeFullBackup', {}, options?.signal, true));
  }

  async takeIncrementalBackup(
    url: string,
    parentBackupId: string,
    options?: CallOptions,
  ): Promise<BackupInfo> {
    return readBackupInfo(
      await this.backupCall(
        url,
        'TakeIncrementalBackup',
        { ParentBackupId: parentBackupId },
        options?.signal,
        true,
      ),
    );
  }

  async takeCoordinatedBackup(url: string, options?: CallOptions): Promise<BackupInfo> {
    return readBackupInfo(
      await this.backupCall(url, 'TakeCoordinatedBackup', {}, options?.signal, true),
    );
  }

  async listBackups(url: string, options?: CallOptions): Promise<BackupInfo[]> {
    const response = await this.backupCall(url, 'ListBackups', {}, options?.signal, false);
    return asArray(response['Backups']).map(readBackupInfo);
  }

  async getBackupChain(
    url: string,
    leafBackupId: string,
    options?: CallOptions,
  ): Promise<BackupInfo[]> {
    const response = await this.backupCall(
      url,
      'GetBackupChain',
      { LeafBackupId: leafBackupId },
      options?.signal,
      false,
    );
    return asArray(response['Backups']).map(readBackupInfo);
  }

  async restore(
    url: string,
    leafBackupId: string,
    targetDir: string,
    targetTimeMs: number,
    options?: CallOptions,
  ): Promise<RestoreResult> {
    const response = await this.backupCall(
      url,
      'Restore',
      { LeafBackupId: leafBackupId, TargetDir: targetDir, TargetTimeMs: targetTimeMs },
      options?.signal,
      true,
    );

    return {
      targetDir: stringOf(response['TargetDir']),
      partitionsRestored: numberOf(response['PartitionsRestored']),
      entriesApplied: numberOf(response['EntriesApplied']),
      lastAppliedPhysicalMs: numberOf(response['LastAppliedPhysicalMs']),
      chain: asArray(response['Chain']).map(readBackupInfo),
      outcome: stringOf(response['Outcome']),
      minRecoverablePhysicalMs: numberOf(response['MinRecoverablePhysicalMs']),
      maxRecoverablePhysicalMs: numberOf(response['MaxRecoverablePhysicalMs']),
    };
  }

  async collectBackupGarbage(
    url: string,
    dryRun: boolean,
    options?: CallOptions,
  ): Promise<BackupGcResult> {
    const response = await this.backupCall(
      url,
      'RunBackupGarbageCollection',
      { DryRun: dryRun },
      options?.signal,
      true,
    );

    return {
      applied: response['Applied'] === true,
      bytesReclaimed: numberOf(response['BytesReclaimed']),
      retentionDeletions: asArray(response['RetentionDeletions']).map((row) => ({
        backupId: stringOf(row['BackupId']),
        type: stringOf(row['Type']),
        createdAtUtc: stringOf(row['CreatedAtUtc']),
        bytes: numberOf(row['Bytes']),
        reason: stringOf(row['Reason']),
      })),
      orphanReclamations: asArray(response['OrphanReclamations']).map((row) => ({
        name: stringOf(row['Name']),
        isDirectory: row['IsDirectory'] === true,
        reason: stringOf(row['Reason']),
      })),
    };
  }

  /**
   * Runs a backup RPC and turns the typed refusal trailer into a typed error. An
   * admin backup RPC also sends the admin key.
   */
  private async backupCall(
    url: string,
    method: string,
    request: Message,
    signal: AbortSignal | undefined,
    admin: boolean,
  ): Promise<Message> {
    try {
      const client = this.services(url).backups;
      return admin
        ? await this.adminCall<Message>(url, client, method, request, signal)
        : await this.call<Message>(url, client, method, request, signal);
    } catch (error) {
      const outcome = readBackupOutcomeTrailer(error);
      if (outcome !== null) {
        throw new KahunaBackupError(outcome, (error as Error).message, { cause: error });
      }
      throw error;
    }
  }
}

// ── gRPC invocation ──────────────────────────────────────────────────────────

interface UnaryCall {
  cancel(): void;
}

function invokeUnary<T>(
  client: grpc.Client,
  method: string,
  request: Message,
  signal: AbortSignal | undefined,
  defaultTimeoutMs: number,
  metadata?: grpc.Metadata,
): Promise<T> {
  throwIfAborted(signal);

  return new Promise<T>((resolve, reject) => {
    const callOptions: grpc.CallOptions = {};
    // A caller that supplied no signal still gets a deadline, so an unresponsive
    // node fails the call instead of hanging it forever.
    if (signal === undefined && defaultTimeoutMs > 0) {
      callOptions.deadline = Date.now() + defaultTimeoutMs;
    }

    const invoke = (client as unknown as Record<string, unknown>)[method];
    if (typeof invoke !== 'function') {
      reject(new TypeError(`gRPC method ${method} is not defined on this service`));
      return;
    }

    let settled = false;
    const callback = (error: grpc.ServiceError | null, response: T): void => {
      settled = true;
      signal?.removeEventListener('abort', onAbort);
      if (error !== null) reject(error);
      else resolve(response);
    };
    const args: unknown[] =
      metadata === undefined
        ? [request, callOptions, callback]
        : [request, metadata, callOptions, callback];
    const call = (invoke as (...args: unknown[]) => UnaryCall).apply(client, args);

    function onAbort(): void {
      if (!settled) call.cancel();
    }

    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

async function* invokeServerStream<T>(
  client: grpc.Client,
  method: string,
  request: Message,
  signal: AbortSignal | undefined,
): AsyncIterable<T> {
  throwIfAborted(signal);

  const invoke = (client as unknown as Record<string, unknown>)[method];
  if (typeof invoke !== 'function') {
    throw new TypeError(`gRPC method ${method} is not defined on this service`);
  }

  const stream = (invoke as (...args: unknown[]) => AsyncIterable<T> & UnaryCall).call(
    client,
    request,
  );

  function onAbort(): void {
    stream.cancel();
  }
  signal?.addEventListener('abort', onAbort, { once: true });

  try {
    for await (const message of stream) yield message;
  } finally {
    signal?.removeEventListener('abort', onAbort);
  }
}

// ── Watch streams ────────────────────────────────────────────────────────────

/** The id of the one watch each call carries. */
const WATCH_ID = 1;

/** `WATCH_REQUEST_OPEN` of the proto's `GrpcWatchRequestType`. */
const WATCH_REQUEST_OPEN = 0;

/**
 * Statuses of a call that ended before the node answered: CANCELLED,
 * DEADLINE_EXCEEDED and UNAVAILABLE. They leave no watch registered, so the
 * opening is safe to repeat.
 */
const RETRYABLE_WATCH_STATUSES = new Set([1, 4, 14]);

interface DuplexCall extends AsyncIterable<Message> {
  write(message: Message): boolean;
  end(): void;
  cancel(): void;
  on(event: 'error', listener: (error: Error) => void): unknown;
}

/**
 * One watch on one call of the bidirectional `Watch` RPC.
 *
 * The call runs on the shared channel to the node. A call is an HTTP/2 stream on
 * that channel, not a connection of its own.
 */
class GrpcWatchStream implements WatchStream {
  readonly #call: DuplexCall;

  readonly #messages: AsyncIterator<Message>;

  /** Frames that arrived in the same message as the answer, delivered before the next read. */
  readonly #pending: WatchFrame[] = [];

  #closed = false;

  private constructor(call: DuplexCall) {
    this.#call = call;
    // The async iterator reports a failed call to its reader. Without a listener of its
    // own, the error a cancelled call emits after its reader left would crash the process.
    call.on('error', () => undefined);
    this.#messages = call[Symbol.asyncIterator]();
  }

  /**
   * Opens the watch and waits for the node's answer. A transport failure before the
   * answer comes back as a `mustRetry` refusal: no watch is registered.
   */
  static async open(
    client: grpc.Client,
    request: WatchRequest,
    signal: AbortSignal | undefined,
  ): Promise<WatchOpening> {
    throwIfAborted(signal);

    const invoke = (client as unknown as Record<string, unknown>)['Watch'];
    if (typeof invoke !== 'function') {
      throw new TypeError('gRPC method Watch is not defined on this service');
    }

    const stream = new GrpcWatchStream((invoke as () => DuplexCall).call(client));

    // The caller's signal bounds only the opening. The open stream lives until it is closed.
    const onAbort = (): void => stream.#call.cancel();
    signal?.addEventListener('abort', onAbort, { once: true });

    try {
      stream.#call.write(watchOpenMessage(request));

      const answer = await stream.#readAnswer();
      if (answer === null) {
        await stream.close();
        return refusedWatchOpening('mustRetry');
      }

      const opening = watchOpeningFromAnswer(answer, stream);
      if (opening.stream === null) await stream.close();
      return opening;
    } catch (error) {
      await stream.close();
      if (signal?.aborted) throw new OperationAbortedError('Operation aborted', { cause: signal.reason });
      if (RETRYABLE_WATCH_STATUSES.has((error as { code?: number }).code ?? -1)) {
        return refusedWatchOpening('mustRetry');
      }
      throw error;
    } finally {
      signal?.removeEventListener('abort', onAbort);
    }
  }

  /** The first frame of the watch is its answer. Null when the call ended before it. */
  async #readAnswer(): Promise<WatchFrame | null> {
    for (;;) {
      const next = await this.#messages.next();
      if (next.done) return null;

      let answer: WatchFrame | null = null;
      for (const wire of asArray(next.value['Frames'])) {
        if (numberOf(wire['WatchId']) !== WATCH_ID) continue;

        const frame = readWatchFrame(wire);
        if (answer === null) answer = frame;
        else this.#pending.push(frame);
      }

      if (answer !== null) return answer;
    }
  }

  async *frames(signal?: AbortSignal): AsyncGenerator<WatchFrame> {
    for (let frame = this.#pending.shift(); frame !== undefined; frame = this.#pending.shift()) {
      yield frame;
      if (isTerminalWatchFrame(frame)) return;
    }

    const onAbort = (): void => this.#call.cancel();
    signal?.addEventListener('abort', onAbort, { once: true });

    try {
      for (;;) {
        let next: IteratorResult<Message>;

        try {
          next = await this.#messages.next();
        } catch {
          next = { done: true, value: undefined };
        }

        if (next.done) {
          if (this.#closed) return;
          if (signal?.aborted) {
            throw new OperationAbortedError('Operation aborted', { cause: signal.reason });
          }
          // The call ended without a terminal frame, so events can be missing from here.
          yield laggedWatchFrame(-1, 'disconnected', HLC_ZERO);
          return;
        }

        for (const wire of asArray(next.value['Frames'])) {
          if (numberOf(wire['WatchId']) !== WATCH_ID) continue;

          const frame = readWatchFrame(wire);
          yield frame;
          if (isTerminalWatchFrame(frame)) return;
        }
      }
    } finally {
      signal?.removeEventListener('abort', onAbort);
    }
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;

    // Ending the request stream ends the call on the server, which closes the watch.
    // Cancelling the call covers a server that no longer reads.
    try {
      this.#call.end();
    } catch {
      // The call already ended.
    }
    this.#call.cancel();
  }
}

function watchOpenMessage(request: WatchRequest): Message {
  const message: Message = {
    Type: WATCH_REQUEST_OPEN,
    WatchId: WATCH_ID,
    ScopeKind: WATCH_SCOPE_KIND_CODES[request.scope.kind],
    ScopeValue: request.scope.value,
    Durability: DURABILITY_CODES[request.durability ?? 'persistent'],
    KeysOnly: request.keysOnly ?? false,
    Capacity: request.capacity ?? 0,
    Partitions: [...(request.partitions ?? [])],
    ...hlcFields('ResumeFrom', request.resumeFrom),
    ResumePartitions: [...(request.resumePartitions ?? [])].map(([partitionId, cursor]) => ({
      PartitionId: partitionId,
      ...hlcFields('Timestamp', cursor),
    })),
  };

  // Absent is unbounded; an empty string is a real bound.
  if (request.scope.startKey !== null) message['StartKey'] = request.scope.startKey;
  if (request.scope.endKey !== null) message['EndKey'] = request.scope.endKey;
  return message;
}

function readWatchFrame(wire: Message): WatchFrame {
  const type = watchFrameTypeFromCode(wire['Type'] as number);
  const partitionId = numberOf(wire['PartitionId']);
  const transactionId = readHlc(wire, 'TransactionId');

  return {
    type,
    partitionId,
    event:
      type === 'event'
        ? {
            key: stringOf(wire['Key']),
            kind: watchEventKindFromCode(wire['Kind'] as number),
            value: readBytes(wire['Value']),
            revision: numberOf(wire['Revision']),
            expires: readHlc(wire, 'Expires'),
            commitTimestamp: readHlc(wire, 'CommitTimestamp'),
            transactionId,
            partitionId,
            origin: typeof wire['Origin'] === 'string' ? wire['Origin'] : null,
          }
        : null,
    timestamp: type === 'progress' || type === 'lagged' ? readHlc(wire, 'Timestamp') : HLC_ZERO,
    transactionId: type === 'transactionEnd' ? transactionId : HLC_ZERO,
    lagReason: type === 'lagged' ? watchLagReasonFromCode(wire['LagReason'] as number) : 'none',
    refusal: type === 'refused' ? keyValueResponseFromCode(wire['Refusal'] as number) : 'get',
    partitions:
      type === 'opened' ? asNumberArray(wire['Partitions']) : [],
    routes:
      type === 'refused'
        ? asArray(wire['Routes']).map((route) => ({
            partitionId: numberOf(route['PartitionId']),
            hostedHere: route['HostedHere'] === true,
            endpoints: Array.isArray(route['Endpoints'])
              ? (route['Endpoints'] as unknown[]).filter((e): e is string => typeof e === 'string')
              : [],
          }))
        : [],
  };
}

// ── Parsing helpers ──────────────────────────────────────────────────────────

function asNumberArray(value: unknown): number[] {
  return Array.isArray(value) ? value.map(numberOf) : [];
}

function numberOf(value: unknown): number {
  return typeof value === 'number' ? value : Number(value ?? 0);
}

function stringOf(value: unknown): string {
  return typeof value === 'string' ? value : '';
}

function stringOrNull(value: unknown): string | null {
  return typeof value === 'string' && value.length > 0 ? value : null;
}

function nameOf(names: readonly string[], value: unknown): string {
  if (typeof value === 'string') return value;
  return names[numberOf(value)] ?? '';
}

function asArray(value: unknown): Message[] {
  return Array.isArray(value) ? (value as Message[]) : [];
}

function readBytes(value: unknown): Uint8Array | null {
  if (value === undefined || value === null) return null;
  if (value instanceof Uint8Array) return value;
  return null;
}

/**
 * Emits an `optional bytes` field only when it carries a payload.
 *
 * Omitting the field is what tells the server "this key holds no value"; sending an
 * empty buffer means "this key holds zero bytes", which is a different key state.
 */
function optionalBytes(name: string, value: Uint8Array | null): Message {
  return value === null ? {} : { [name]: Buffer.from(value) };
}

function hlcFields(prefix: string, value: HlcTimestamp | undefined): Message {
  const source = value ?? HLC_ZERO;
  return {
    [`${prefix}Node`]: source.node,
    [`${prefix}Physical`]: source.physical,
    [`${prefix}Counter`]: source.counter,
  };
}

function readHlc(message: Message, prefix: string): HlcTimestamp {
  return {
    node: numberOf(message[`${prefix}Node`]),
    physical: numberOf(message[`${prefix}Physical`]),
    counter: numberOf(message[`${prefix}Counter`]),
  };
}

function transactionFields(options: TransactionalCallOptions | undefined): Message {
  const transaction = options?.transaction;
  return {
    ...hlcFields('TransactionId', transaction?.transactionId ?? HLC_ZERO),
    CoordinatorKey: transaction?.coordinatorKey ?? '',
    OperationIdHigh: (transaction?.operationId ?? NO_OPERATION_ID).high,
    OperationIdLow: (transaction?.operationId ?? NO_OPERATION_ID).low,
  };
}

function rangeFields(bounds: RangeBounds): Message {
  const fields: Message = {
    Prefix: bounds.prefix,
    StartInclusive: bounds.startInclusive ?? true,
    EndInclusive: bounds.endInclusive ?? false,
  };
  // Absent means an open end; an empty string is a real bound, so the distinction
  // is carried by presence rather than by an empty value.
  if (bounds.startKey !== null && bounds.startKey !== undefined) fields['StartKey'] = bounds.startKey;
  if (bounds.endKey !== null && bounds.endKey !== undefined) fields['EndKey'] = bounds.endKey;
  return fields;
}

function normalizeValue(value: string | Uint8Array | null): Uint8Array | null {
  if (value === null) return null;
  return typeof value === 'string' ? new TextEncoder().encode(value) : value;
}

function readRouteHint(value: unknown): RouteHint | null {
  if (value === null || value === undefined || typeof value !== 'object') return null;
  const hint = value as Message;
  return {
    partitionId: numberOf(hint['PartitionId']),
    endpoint: stringOf(hint['Endpoint']),
    provenance: routeProvenanceFromCode(hint['Provenance'] as number),
    generation: numberOf(hint['Generation']),
  };
}

function readRouteTable(value: unknown): RouteHint[] | null {
  if (!Array.isArray(value)) return null;
  const table: RouteHint[] = [];
  for (const entry of value) {
    const hint = readRouteHint(entry);
    if (hint !== null) table.push(hint);
  }
  return table;
}

function readBatchWriteItems(value: unknown): BatchWriteResult[] {
  return asArray(value).map((row) => ({
    key: stringOf(row['Key']),
    type: keyValueResponseFromCode(row['Type'] as number),
    revision: numberOf(row['Revision']),
    lastModified: readHlc(row, 'LastModified'),
    durability: durabilityFromCode(row['Durability'] as number),
    routeIndex: numberOf(row['RouteIndex']),
    holderTransactionId: readHlc(row, 'HolderTransactionId'),
  }));
}

function readScanItems(value: unknown): ScanItem[] {
  return asArray(value).map((item) => ({
    key: stringOf(item['Key']),
    value: readBytes(item['Value']),
    revision: numberOf(item['Revision']),
    lastModified: readHlc(item, 'LastModified'),
  }));
}

function readBucketItems(response: Message, prefixKey: string): ScanItem[] {
  const type = keyValueResponseFromCode(response['Type'] as number);
  if (type === 'get') return readScanItems(response['Items']);
  if (type === 'doesNotExist') return [];
  throw KahunaError.keyValue(`Failed to scan key/values for '${prefixKey}': ${type}.`, type);
}

function readSequenceEntry(value: unknown): SequenceEntry | null {
  if (value === null || value === undefined || typeof value !== 'object') return null;
  const entry = value as Message;
  if (!entry['Name']) return null;

  const maxValue = entry['MaxValue'];
  const blockSize = entry['BlockSize'];
  return {
    name: stringOf(entry['Name']),
    currentValue: numberOf(entry['CurrentValue']),
    initialValue: numberOf(entry['InitialValue']),
    increment: numberOf(entry['Increment']),
    maxValue: maxValue === undefined || maxValue === null ? null : numberOf(maxValue),
    blockSize: blockSize === undefined || blockSize === null ? null : numberOf(blockSize),
    incarnation: numberOf(entry['Incarnation']),
    revision: numberOf(entry['Revision']),
    durability: 'persistent',
    createdAt: readHlc(entry, 'CreatedAt'),
    updatedAt: readHlc(entry, 'UpdatedAt'),
  };
}

function readBackupInfo(value: Message): BackupInfo {
  const hasCoverage = value['HasCoverage'] === true;
  return {
    backupId: stringOf(value['BackupId']),
    formatVersion: numberOf(value['FormatVersion']),
    type: stringOf(value['Type']),
    createdAtUtc: stringOf(value['CreatedAtUtc']),
    parentBackupId: stringOrNull(value['ParentBackupId']),
    partitionCount: numberOf(value['PartitionCount']),
    clusterId: stringOrNull(value['ClusterId']),
    coordinatorNode: stringOrNull(value['CoordinatorNode']),
    requestedKind: stringOrNull(value['RequestedKind']),
    actualKind: stringOrNull(value['ActualKind']),
    substitutionReason: stringOrNull(value['SubstitutionReason']),
    isInvalid: value['IsInvalid'] === true,
    isIncomplete: value['IsIncomplete'] === true,
    invalidReason: stringOrNull(value['InvalidReason']),
    minRecoverablePhysicalMs: hasCoverage ? numberOf(value['MinRecoverablePhysicalMs']) : null,
    maxRecoverablePhysicalMs: hasCoverage ? numberOf(value['MaxRecoverablePhysicalMs']) : null,
  };
}

function readBackupOutcomeTrailer(error: unknown): string | null {
  const metadata = (error as { metadata?: grpc.Metadata } | null)?.metadata;
  if (!metadata) return null;

  const values = metadata.get(BACKUP_OUTCOME_TRAILER);
  const first = values[0];
  return typeof first === 'string' ? first : null;
}

export { BACKUP_OUTCOME_TRAILER };
