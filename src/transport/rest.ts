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
  durabilityFromCode,
  keyValueResponseFromCode,
  lockResponseFromCode,
  routeProvenanceFromCode,
  sequenceResponseFromCode,
  type Durability,
  type KeyValueResponseCode,
  type RangeLockMode,
  type RoutingDomain,
  type TransactionPriority,
} from '../enums.js';
import { KahunaBackupError, KahunaError, isAbortError, throwIfAborted } from '../errors.js';
import { HLC_ZERO, hlcFromJson, hlcToJson, type HlcJson, type HlcTimestamp } from '../hlc.js';
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
  WriteOutcome,
} from '../types.js';
import { NO_OPERATION_ID, isOperationIdEmpty } from '../types.js';
import { payloadFromJson, payloadToJson, requiredPayloadToJson } from './codec.js';
import { HttpClient, HttpStatusError, isRetriableStatus } from './http.js';
import {
  LazyBackoff,
  MUST_RETRY_ATTEMPTS,
  decorrelatedJitterBackoff,
  delay,
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
} from './transport.js';

/** The header a backup endpoint puts its typed outcome in. */
const BACKUP_OUTCOME_HEADER = 'x-kahuna-backup-outcome';

interface JsonRecord {
  [key: string]: unknown;
}

/**
 * Talks to Kahuna over its JSON/HTTP surface.
 *
 * Every endpoint is a POST of one JSON body, except the few read-only endpoints
 * that are GETs. The transport owns the `mustRetry` loops, because `mustRetry`
 * describes a transient server condition rather than a failure the caller should
 * see.
 */
export class RestTransport implements Transport {
  readonly kind = 'rest' as const;

  routeSink: RouteSink | null = null;

  private readonly http: HttpClient;

  private readonly bearerToken: string;

  constructor(
    options: SecurityOptions & { bearerToken?: string; useHttp2?: boolean } = {},
  ) {
    this.http = new HttpClient(options, options.useHttp2 ?? false);
    this.bearerToken = options.bearerToken ?? 'xxx';
  }

  async close(): Promise<void> {
    await this.http.close();
  }

  // ── Plumbing ───────────────────────────────────────────────────────────────

  private learnRoute(
    domain: RoutingDomain,
    resource: string,
    response: JsonRecord,
    url: string,
  ): void {
    const sink = this.routeSink;
    if (sink === null) return;
    sink.learn(domain, resource, readRouteHint(response['route']), url);
  }

  private learnBatchRoutes(
    items: readonly { key: string; routeIndex: number }[],
    routes: unknown,
    url: string,
  ): void {
    const sink = this.routeSink;
    if (sink === null) return;
    sink.learnBatch('keyValue', items, readRouteTable(routes), url);
  }

  /**
   * Sends one request, repeating it while the failure looks transient.
   *
   * A failure that outlives the retry budget also marks the endpoint, so later
   * operations stop queueing behind a node that is down.
   */
  private async send(
    url: string,
    path: string,
    method: 'GET' | 'POST',
    body: unknown,
    signal: AbortSignal | undefined,
  ): Promise<JsonRecord> {
    const target = joinUrl(url, path);
    const encoded =
      body === undefined ? undefined : new TextEncoder().encode(JSON.stringify(body));

    const backoff = decorrelatedJitterBackoff(1_000, 5);
    let attempt = 0;

    for (;;) {
      throwIfAborted(signal);
      try {
        const response = await this.http.send({
          method,
          url: target,
          body: encoded,
          bearerToken: this.bearerToken,
          signal,
        });

        if (response.status >= 400) {
          throw new HttpStatusError(response.status, response.body, target, response.headers);
        }

        return response.body.length === 0 ? {} : (JSON.parse(response.body) as JsonRecord);
      } catch (error) {
        if (isAbortError(error)) throw error;

        const retriable =
          attempt < 5 &&
          (!(error instanceof HttpStatusError) || isRetriableStatus(error.status));

        if (!retriable) {
          this.routeSink?.reportEndpointFailure(url);
          throw error;
        }

        attempt++;
        const next = backoff.next();
        await delay(next.done ? 1_000 : next.value, signal);
      }
    }
  }

  /** Sends a key/value request and repeats it while the server answers `mustRetry`. */
  private async postKeyValue<T extends JsonRecord>(
    url: string,
    verb: string,
    body: JsonRecord,
    signal: AbortSignal | undefined,
    carryForward?: (request: JsonRecord, response: T) => void,
  ): Promise<T> {
    for (let attempt = 0; ; attempt++) {
      const response = (await this.send(url, `v1/kv/${verb}`, 'POST', body, signal)) as T;

      if (keyValueResponseFromCode(response['type'] as number) !== 'mustRetry') return response;

      carryForward?.(body, response);

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
    const body = {
      resource,
      owner: requiredPayloadToJson(owner),
      expiresMs: expiryMs,
      durability: DURABILITY_CODES[durability],
    };

    // `mustRetry` is unbounded here, the same policy the gRPC transport keeps. The
    // caller chose to wait for this lock, so termination is its own signal's job.
    const backoff = new LazyBackoff();

    for (;;) {
      throwIfAborted(options?.signal);

      const response = await this.send(url, 'v1/locks/try-lock', 'POST', body, options?.signal);
      this.learnRoute('lock', resource, response, url);

      const type = lockResponseFromCode(response['type'] as number);
      const fencingToken = numberOf(response['fencingToken']);
      const servedFrom = stringOrNull(response['servedFrom']);

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
    const body = {
      resource,
      owner: requiredPayloadToJson(owner),
      durability: DURABILITY_CODES[durability],
    };

    return this.lockMustRetryLoop(url, 'v1/locks/try-unlock', body, resource, options, (type) => {
      if (type === 'unlocked') return { value: true };
      if (type === 'lockDoesNotExist') return { value: false };
      if (type !== 'mustRetry') {
        throw KahunaError.lock(`Failed to unlock: ${type}`, type);
      }
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
    const body = {
      resource,
      owner: requiredPayloadToJson(owner),
      expiresMs: expiryMs,
      durability: DURABILITY_CODES[durability],
    };

    return this.lockMustRetryLoop(
      url,
      'v1/locks/try-extend',
      body,
      resource,
      options,
      (type, response) => {
        if (type === 'extended') {
          return { value: { extended: true, fencingToken: numberOf(response['fencingToken']) } };
        }
        if (type !== 'mustRetry') {
          throw KahunaError.lock(`Failed to extend lock: ${type}`, type);
        }
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
    const body = { resource, durability: DURABILITY_CODES[durability] };

    return this.lockMustRetryLoop(
      url,
      'v1/locks/get-info',
      body,
      resource,
      options,
      (type, response) => {
        if (type === 'got') {
          return {
            value: {
              owner: payloadFromJson(response['owner'] as string | null),
              expires: hlcFromJson(response['expires'] as HlcJson | null),
              fencingToken: numberOf(response['fencingToken']),
            } satisfies LockInfo,
          };
        }
        if (type !== 'mustRetry') {
          throw KahunaError.lock(`Failed to get lock information: ${type}`, type);
        }
        return null;
      },
    );
  }

  /**
   * Repeats a lock verb while the server answers `mustRetry`, bounded by a deadline.
   *
   * A fixed count of attempts would spend its whole budget in the first few
   * milliseconds, and a release that gives up leaves the lock held until it expires.
   */
  private async lockMustRetryLoop<T>(
    url: string,
    path: string,
    body: JsonRecord,
    resource: string,
    options: CallOptions | undefined,
    decide: (
      type: ReturnType<typeof lockResponseFromCode>,
      response: JsonRecord,
    ) => { value: T } | null,
  ): Promise<T> {
    let retries = 0;
    let deadline = 0;

    for (;;) {
      throwIfAborted(options?.signal);

      const response = await this.send(url, path, 'POST', body, options?.signal);
      this.learnRoute('lock', resource, response, url);

      const decided = decide(lockResponseFromCode(response['type'] as number), response);
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
      key,
      value: payloadToJson(value),
      expiresMs: expiryMs,
      flags,
      durability: DURABILITY_CODES[durability],
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
      key,
      value: payloadToJson(value),
      compareValue: payloadToJson(compareValue),
      expiresMs: expiryMs,
      flags: SET_IF_EQUAL_TO_VALUE_FLAG,
      durability: DURABILITY_CODES[durability],
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
      key,
      value: payloadToJson(value),
      compareRevision,
      expiresMs: expiryMs,
      flags: SET_IF_EQUAL_TO_REVISION_FLAG,
      durability: DURABILITY_CODES[durability],
    });
  }

  /** Runs one `try-set` and classifies its answer exactly as the .NET client does. */
  private async writeKeyValue(
    url: string,
    key: string,
    options: TransactionalCallOptions | undefined,
    body: JsonRecord,
  ): Promise<WriteOutcome> {
    let retries = 0;
    let type: KeyValueResponseCode;

    do {
      throwIfAborted(options?.signal);

      const response = await this.send(url, 'v1/kv/try-set', 'POST', body, options?.signal);
      this.learnRoute('keyValue', key, response, url);

      type = keyValueResponseFromCode(response['type'] as number);
      const revision = numberOf(response['revision']);

      if (type === 'set') return { success: true, revision, timeElapsedMs: 0 };
      if (type === 'notSet') return { success: false, revision, timeElapsedMs: 0 };

      if (++retries >= MUST_RETRY_ATTEMPTS) {
        throw KahunaError.keyValue('Retries exhausted.', 'mustRetry');
      }
      if (type === 'mustRetry') await waitBeforeMustRetry(retries - 1, options?.signal);
    } while (type === 'mustRetry');

    throw KahunaError.keyValue(`Failed to set key/value: ${type}`, type);
  }

  async get(
    url: string,
    key: string,
    revision: number,
    durability: Durability,
    options?: ReadCallOptions,
  ): Promise<ReadOutcome> {
    const body: JsonRecord = {
      ...transactionFields(options),
      key,
      revision,
      readTimestamp: hlcToJson(options?.readTimestamp),
      durability: DURABILITY_CODES[durability],
    };

    let retries = 0;
    let type: KeyValueResponseCode;

    do {
      throwIfAborted(options?.signal);

      const response = await this.send(url, 'v1/kv/try-get', 'POST', body, options?.signal);
      this.learnRoute('keyValue', key, response, url);

      type = keyValueResponseFromCode(response['type'] as number);
      const responseRevision = numberOf(response['revision']);

      if (type === 'get') {
        return {
          success: true,
          value: payloadFromJson(response['value'] as string | null),
          revision: responseRevision,
          lastModified: hlcFromJson(response['lastModified'] as HlcJson | null),
          timeElapsedMs: 0,
        };
      }
      if (type === 'doesNotExist') {
        return {
          success: false,
          value: null,
          revision: responseRevision,
          lastModified: HLC_ZERO,
          timeElapsedMs: 0,
        };
      }

      if (++retries >= MUST_RETRY_ATTEMPTS) {
        throw KahunaError.keyValue('Retries exhausted.', 'mustRetry');
      }
      if (type === 'mustRetry') await waitBeforeMustRetry(retries - 1, options?.signal);
    } while (type === 'mustRetry');

    throw KahunaError.keyValue(`Failed to get key/value: ${type}`, type);
  }

  async exists(
    url: string,
    key: string,
    revision: number,
    durability: Durability,
    options?: ReadCallOptions,
  ): Promise<WriteOutcome> {
    const body: JsonRecord = {
      ...transactionFields(options),
      key,
      revision,
      readTimestamp: hlcToJson(options?.readTimestamp),
      durability: DURABILITY_CODES[durability],
    };

    return this.pointOutcome(url, key, 'try-exists', body, options, 'exists', (type) =>
      KahunaError.keyValue(`Failed to check if exists key/value: ${type}`, type),
    );
  }

  async delete(
    url: string,
    key: string,
    durability: Durability,
    options?: TransactionalCallOptions,
  ): Promise<WriteOutcome> {
    const body: JsonRecord = {
      ...transactionFields(options),
      key,
      durability: DURABILITY_CODES[durability],
    };

    return this.pointOutcome(url, key, 'try-delete', body, options, 'deleted', (type) =>
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
    const body: JsonRecord = {
      ...transactionFields(options),
      key,
      expiresMs: expiryMs,
      durability: DURABILITY_CODES[durability],
    };

    return this.pointOutcome(url, key, 'try-extend', body, options, 'extended', (type) =>
      KahunaError.keyValue(`Failed to extend key/value: ${type}`, type),
    );
  }

  /** The shared shape of `exists`, `delete` and `extend`: one success code, one miss code. */
  private async pointOutcome(
    url: string,
    key: string,
    verb: string,
    body: JsonRecord,
    options: CallOptions | undefined,
    successCode: KeyValueResponseCode,
    failure: (type: KeyValueResponseCode) => KahunaError,
  ): Promise<WriteOutcome> {
    let retries = 0;
    let type: KeyValueResponseCode;

    do {
      throwIfAborted(options?.signal);

      const response = await this.send(url, `v1/kv/${verb}`, 'POST', body, options?.signal);
      this.learnRoute('keyValue', key, response, url);

      type = keyValueResponseFromCode(response['type'] as number);
      const revision = numberOf(response['revision']);

      if (type === successCode) return { success: true, revision, timeElapsedMs: 0 };
      if (type === 'doesNotExist') return { success: false, revision, timeElapsedMs: 0 };

      if (++retries >= MUST_RETRY_ATTEMPTS) {
        throw KahunaError.keyValue('Retries exhausted.', 'mustRetry');
      }
      if (type === 'mustRetry') await waitBeforeMustRetry(retries - 1, options?.signal);
    } while (type === 'mustRetry');

    throw failure(type);
  }

  // ── Key/value batches ──────────────────────────────────────────────────────

  async setMany(
    url: string,
    items: readonly SetManyItem[],
    options?: CallOptions,
  ): Promise<BatchOutcome<BatchWriteResult>> {
    throwIfAborted(options?.signal);

    const body = {
      items: items.map((item) => ({
        transactionId: hlcToJson(HLC_ZERO),
        key: item.key,
        value: payloadToJson(item.value === undefined ? null : normalizeValue(item.value)),
        compareValue: payloadToJson(
          item.compareValue === undefined ? null : normalizeValue(item.compareValue),
        ),
        compareRevision: item.compareRevision ?? 0,
        expiresMs: item.expiry ?? 0,
        flags: item.flags ?? 1,
        durability: DURABILITY_CODES[item.durability ?? 'persistent'],
      })),
    };

    const response = await this.send(url, 'v1/kv/try-set-many', 'POST', body, options?.signal);
    return this.readBatchWrite(response, url, 'TrySetManyKeyValues failed');
  }

  async deleteMany(
    url: string,
    items: readonly DeleteManyItem[],
    options?: TransactionalCallOptions,
  ): Promise<BatchOutcome<BatchWriteResult>> {
    throwIfAborted(options?.signal);

    const transaction = options?.transaction;
    const body: JsonRecord = {
      items: items.map((item) => ({
        transactionId: hlcToJson(transaction?.transactionId ?? HLC_ZERO),
        key: item.key,
        durability: DURABILITY_CODES[item.durability ?? 'persistent'],
      })),
    };

    // The whole batch registers as one coordinator operation, so the persistent keys
    // it confirms anchor the transaction record deterministically.
    if (transaction && transaction.coordinatorKey && !isOperationIdEmpty(transaction.operationId)) {
      body['coordinatorKey'] = transaction.coordinatorKey;
      body['operationIdHigh'] = transaction.operationId.high;
      body['operationIdLow'] = transaction.operationId.low;
    }

    const response = await this.send(url, 'v1/kv/try-delete-many', 'POST', body, options?.signal);
    return this.readBatchWrite(response, url, 'TryDeleteManyKeyValues failed');
  }

  async getMany(
    url: string,
    items: readonly GetManyItem[],
    options?: ReadCallOptions,
  ): Promise<BatchOutcome<BatchReadResult>> {
    return this.readManyKeyValues(url, 'try-get-many', items, options);
  }

  async existsMany(
    url: string,
    items: readonly GetManyItem[],
    options?: ReadCallOptions,
  ): Promise<BatchOutcome<BatchReadResult>> {
    return this.readManyKeyValues(url, 'try-exists-many', items, options);
  }

  private async readManyKeyValues(
    url: string,
    verb: string,
    items: readonly GetManyItem[],
    options: ReadCallOptions | undefined,
  ): Promise<BatchOutcome<BatchReadResult>> {
    throwIfAborted(options?.signal);

    const body: JsonRecord = {
      transactionId: hlcToJson(options?.transaction?.transactionId ?? HLC_ZERO),
      readTimestamp: hlcToJson(options?.readTimestamp),
      items: items.map((item) => ({
        key: item.key,
        revision: item.revision ?? -1,
        durability: DURABILITY_CODES[item.durability ?? 'persistent'],
      })),
    };

    const response = await this.send(url, `v1/kv/${verb}`, 'POST', body, options?.signal);

    const envelope = keyValueResponseFromCode(response['type'] as number);
    // A retryable server-side failure arrives as HTTP 200 whose body is only the
    // envelope code. Returning its absent item list as an empty batch would read as
    // "none of these keys exist" instead of "nothing was measured".
    if (envelope === 'mustRetry' || envelope === 'errored') {
      throw KahunaError.keyValue(`${verb} failed`, envelope);
    }

    const rows = (response['items'] as JsonRecord[] | null) ?? [];
    const parsed: BatchReadResult[] = rows.map((row) => ({
      key: stringOf(row['key']),
      type: keyValueResponseFromCode(row['type'] as number),
      value: payloadFromJson(row['value'] as string | null),
      revision: numberOf(row['revision']),
      lastModified: hlcFromJson(row['lastModified'] as HlcJson | null),
      durability: durabilityFromCode(row['durability'] as number),
      routeIndex: numberOf(row['routeIndex']),
    }));

    this.learnBatchRoutes(parsed, response['routes'], url);
    return { items: parsed, timeElapsedMs: numberOf(response['timeElapsedMs']) };
  }

  private readBatchWrite(
    response: JsonRecord,
    url: string,
    failureMessage: string,
  ): BatchOutcome<BatchWriteResult> {
    const envelope = keyValueResponseFromCode(response['type'] as number);
    if (envelope === 'mustRetry' || envelope === 'errored') {
      throw KahunaError.keyValue(failureMessage, envelope);
    }

    const rows = (response['items'] as JsonRecord[] | null) ?? [];
    const parsed: BatchWriteResult[] = rows.map((row) => ({
      key: stringOf(row['key']),
      type: keyValueResponseFromCode(row['type'] as number),
      revision: numberOf(row['revision']),
      lastModified: hlcFromJson(row['lastModified'] as HlcJson | null),
      durability: durabilityFromCode(row['durability'] as number),
      routeIndex: numberOf(row['routeIndex']),
    }));

    this.learnBatchRoutes(parsed, response['routes'], url);
    return { items: parsed, timeElapsedMs: numberOf(response['timeElapsedMs']) };
  }

  // ── Scans ──────────────────────────────────────────────────────────────────

  async getByBucket(
    url: string,
    prefixKey: string,
    durability: Durability,
    options?: ReadCallOptions,
  ): Promise<ScanItem[]> {
    const response = await this.postKeyValue(
      url,
      'get-by-bucket',
      {
        ...transactionFields(options),
        prefixKey,
        readTimestamp: hlcToJson(options?.readTimestamp),
        durability: DURABILITY_CODES[durability],
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
    const response = await this.postKeyValue(
      url,
      'scan-all-by-prefix',
      {
        prefixKey,
        readTimestamp: hlcToJson(options?.readTimestamp),
        durability: DURABILITY_CODES[durability],
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
    options?: ReadCallOptions & { cursor?: string | null },
  ): Promise<RangePage> {
    const response = await this.postKeyValue(
      url,
      'get-by-range',
      {
        ...transactionFields(options),
        ...rangeFields(bounds),
        limit,
        readTimestamp: hlcToJson(options?.readTimestamp),
        durability: DURABILITY_CODES[durability],
        cursor: options?.cursor ?? null,
      },
      options?.signal,
    );

    const type = keyValueResponseFromCode(response['type'] as number);
    if (type !== 'get' && type !== 'doesNotExist') {
      throw KahunaError.keyValue(`Failed to get by range for '${bounds.prefix}': ${type}.`, type);
    }

    return {
      items: readScanItems(response['items']),
      nextCursor: stringOrNull(response['nextCursor']),
      hasMore: response['hasMore'] === true,
    };
  }

  async *scanByRange(
    url: string,
    bounds: RangeBounds,
    pageSize: number,
    durability: Durability,
    options?: ReadCallOptions,
  ): AsyncIterable<ScanItem> {
    let cursor: string | null = null;

    for (;;) {
      const page: RangePage = await this.getByRange(url, bounds, pageSize, durability, {
        ...options,
        cursor,
      });

      yield* page.items;

      if (!page.hasMore || !page.nextCursor) return;
      cursor = page.nextCursor;
    }
  }

  // ── Transaction locks ──────────────────────────────────────────────────────

  async acquireExclusiveLock(
    url: string,
    key: string,
    expiryMs: number,
    durability: Durability,
    options: TransactionalCallOptions,
  ): Promise<boolean> {
    const response = await this.postKeyValue(
      url,
      'try-acquire-exclusive-lock',
      {
        ...transactionFields(options),
        key,
        expiresMs: expiryMs,
        durability: DURABILITY_CODES[durability],
      },
      options.signal,
    );

    const type = keyValueResponseFromCode(response['type'] as number);
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
    const response = await this.postKeyValue(
      url,
      'try-acquire-prefix-lock',
      {
        ...transactionFields(options),
        key: prefixKey,
        expiresMs: expiryMs,
        durability: DURABILITY_CODES[durability],
      },
      options.signal,
    );

    const type = keyValueResponseFromCode(response['type'] as number);
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
    await this.send(
      url,
      'v1/kv/try-release-prefix-lock',
      'POST',
      {
        transactionId: hlcToJson(options.transaction?.transactionId ?? HLC_ZERO),
        key: prefixKey,
        durability: DURABILITY_CODES[durability],
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
    const response = await this.postKeyValue(
      url,
      'try-acquire-range-lock',
      {
        ...transactionFields(options),
        ...rangeFields(request),
        expiresMs: expiryMs,
        durability: DURABILITY_CODES[durability],
        mode: RANGE_LOCK_MODE_CODES[mode],
      },
      options.signal,
    );

    const type = keyValueResponseFromCode(response['type'] as number);
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
    await this.send(
      url,
      'v1/kv/try-release-range-lock',
      'POST',
      {
        transactionId: hlcToJson(options.transaction?.transactionId ?? HLC_ZERO),
        ...rangeFields(bounds),
        durability: DURABILITY_CODES[durability],
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
    const body = {
      hash,
      script: Buffer.from(script).toString('base64'),
      parameters: parameters === null ? null : parameters.map((p) => ({ key: p.key, value: p.value })),
      priority: TRANSACTION_PRIORITY_CODES[priority],
    };

    let retries = 0;
    let type: KeyValueResponseCode;
    let reason: string | null = null;

    do {
      throwIfAborted(options?.signal);

      const response = await this.send(
        url,
        'v1/kv/try-execute-tx-script',
        'POST',
        body,
        options?.signal,
      );

      type = keyValueResponseFromCode(response['type'] as number);
      reason = stringOrNull(response['reason']);
      const code = response['type'] as number;

      // Anything below the error band, plus the explicit "no such key", is a result
      // the script produced rather than a failure of running it.
      if (code < 99 || type === 'doesNotExist') {
        return {
          type,
          servedFrom: stringOrNull(response['servedFrom']),
          values: readScriptValues(response['values']),
          timeElapsedMs: 0,
        };
      }

      if (++retries >= MUST_RETRY_ATTEMPTS) {
        throw KahunaError.keyValue('Retries exhausted.', 'mustRetry');
      }
      if (type === 'mustRetry') await waitBeforeMustRetry(retries - 1, options?.signal);
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
    const response = await this.postKeyValue(
      url,
      'start-tx-session',
      {
        coordinatorKey,
        timeout: options.timeout ?? 0,
        admissionWaitMs: options.admissionWaitMs ?? 0,
        lockingType: TRANSACTION_LOCKING_CODES[options.locking ?? 'pessimistic'],
        asyncRelease: options.asyncRelease ?? false,
        autoCommit: options.autoCommit ?? true,
        readValidation: READ_VALIDATION_CODES[options.readValidation ?? 'none'],
        decisionDurability: DECISION_DURABILITY_CODES[options.decisionDurability ?? 'bestEffort'],
        priority: TRANSACTION_PRIORITY_CODES[options.priority ?? 'normal'],
        conflictPolicy: TRANSACTION_CONFLICT_POLICY_CODES[options.conflictPolicy ?? 'normal'],
        readTimestamp: hlcToJson(options.readTimestamp),
      },
      call?.signal,
    );

    const type = keyValueResponseFromCode(response['type'] as number);
    if (type === 'set') {
      return { url, transactionId: hlcFromJson(response['transactionId'] as HlcJson) };
    }
    throw KahunaError.keyValue(`Failed to start key/value transaction: ${type}`, type);
  }

  async commitTransaction(
    url: string,
    coordinatorKey: string,
    transactionId: HlcTimestamp,
    recordAnchorKey: string | null,
    call?: CallOptions,
  ): Promise<TransactionCommit> {
    const response = await this.postKeyValue(
      url,
      'commit-tx-session',
      { coordinatorKey, transactionId: hlcToJson(transactionId), recordAnchorKey },
      call?.signal,
      // A retry must carry forward the anchor the server minted, or the retry looks
      // like a different transaction to the coordinator.
      (request, retryResponse) => {
        const anchor = stringOrNull(retryResponse['recordAnchorKey']);
        if (anchor !== null) request['recordAnchorKey'] = anchor;
      },
    );

    const type = keyValueResponseFromCode(response['type'] as number);
    if (type === 'committed') {
      return { committed: true, recordAnchorKey: stringOrNull(response['recordAnchorKey']) };
    }
    throw KahunaError.keyValue(`Failed to commit key/value transaction: ${type}`, type);
  }

  async rollbackTransaction(
    url: string,
    coordinatorKey: string,
    transactionId: HlcTimestamp,
    recordAnchorKey: string | null,
    call?: CallOptions,
  ): Promise<boolean> {
    const response = await this.postKeyValue(
      url,
      'rollback-tx-session',
      { coordinatorKey, transactionId: hlcToJson(transactionId), recordAnchorKey },
      call?.signal,
    );

    const type = keyValueResponseFromCode(response['type'] as number);
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
    return this.sequenceCall(url, 'create', name, options, {
      name,
      initialValue,
      increment,
      maxValue,
      blockSize,
      durability: 1,
    });
  }

  async updateSequence(
    url: string,
    name: string,
    update: SequenceUpdate,
    options?: CallOptions,
  ): Promise<SequenceOutcome> {
    // A null field leaves that parameter of the sequence as it is.
    return this.sequenceCall(url, 'update', name, options, {
      name,
      currentValue: update.currentValue ?? null,
      increment: update.increment ?? null,
      initialValue: update.initialValue ?? null,
      maxValue: update.maxValue ?? null,
      removeMaxValue: update.removeMaxValue === true,
      blockSize: update.blockSize ?? null,
      removeBlockSize: update.removeBlockSize === true,
      durability: 1,
    });
  }

  async getSequence(url: string, name: string, options?: CallOptions): Promise<SequenceOutcome> {
    return this.sequenceCall(url, 'get', name, options, { name, durability: 1 });
  }

  async deleteSequence(url: string, name: string, options?: CallOptions): Promise<SequenceOutcome> {
    return this.sequenceCall(url, 'delete', name, options, { name, durability: 1 });
  }

  async nextSequenceValue(
    url: string,
    name: string,
    idempotencyKey: string | null,
    options?: CallOptions,
  ): Promise<SequenceAllocationOutcome> {
    return this.sequenceAllocationCall(url, 'next', name, options, {
      name,
      idempotencyKey,
      durability: 1,
    });
  }

  async reserveSequenceRange(
    url: string,
    name: string,
    count: number,
    idempotencyKey: string | null,
    options?: CallOptions,
  ): Promise<SequenceAllocationOutcome> {
    return this.sequenceAllocationCall(url, 'reserve', name, options, {
      name,
      count,
      idempotencyKey,
      durability: 1,
    });
  }

  private async sequenceCall(
    url: string,
    action: string,
    name: string,
    options: CallOptions | undefined,
    body: JsonRecord,
  ): Promise<SequenceOutcome> {
    const started = monotonicNow();
    const response = await this.send(
      url,
      `v1/sequences/${action}`,
      'POST',
      body,
      options?.signal,
    );
    this.learnRoute('sequence', name, response, url);

    return {
      type: sequenceResponseFromCode(response['type'] as number),
      entry: readSequenceEntry(response['sequence']),
      revision: numberOf(response['revision']),
      timeElapsedMs: monotonicNow() - started,
    };
  }

  private async sequenceAllocationCall(
    url: string,
    action: string,
    name: string,
    options: CallOptions | undefined,
    body: JsonRecord,
  ): Promise<SequenceAllocationOutcome> {
    const started = monotonicNow();
    const response = await this.send(
      url,
      `v1/sequences/${action}`,
      'POST',
      body,
      options?.signal,
    );
    this.learnRoute('sequence', name, response, url);

    const allocation = (response['allocation'] as JsonRecord | null) ?? {};
    return {
      type: sequenceResponseFromCode(response['type'] as number),
      allocation: {
        name: stringOf(allocation['name']),
        start: numberOf(allocation['start']),
        end: numberOf(allocation['end']),
        count: numberOf(allocation['count']),
        revision: numberOf(allocation['revision']),
      },
      timeElapsedMs: monotonicNow() - started,
    };
  }

  // ── Key ranges ─────────────────────────────────────────────────────────────

  async registerKeyRange(
    url: string,
    keySpace: string,
    options?: CallOptions,
  ): Promise<RegisterKeyRangeResult> {
    const response = await this.send(
      url,
      'v1/ranges/register',
      'POST',
      { keySpace },
      options?.signal,
    );

    if (!response['status']) {
      throw KahunaError.lock('RegisterKeyRange returned no outcome', 'errored');
    }

    return {
      success: response['success'] === true,
      status: stringOf(response['status']),
      seeded: response['seeded'] === true,
      routingMode: stringOf(response['routingMode']),
      descriptorCount: numberOf(response['descriptorCount']),
      reason: stringOrNull(response['reason']),
    };
  }

  async removeKeyRange(
    url: string,
    keySpace: string,
    options?: CallOptions,
  ): Promise<RemoveKeyRangeResult> {
    const response = await this.send(
      url,
      'v1/ranges/unregister',
      'POST',
      { keySpace },
      options?.signal,
    );

    if (!response['status']) {
      throw KahunaError.lock('RemoveKeyRange returned no outcome', 'errored');
    }

    return {
      success: response['success'] === true,
      status: stringOf(response['status']),
      routingMode: stringOf(response['routingMode']),
      descriptorCount: numberOf(response['descriptorCount']),
      reason: stringOrNull(response['reason']),
    };
  }

  async getRanges(
    url: string,
    keySpace: string | null,
    options?: CallOptions,
  ): Promise<RangeMap> {
    const path = keySpace ? `v1/ranges?keySpace=${encodeURIComponent(keySpace)}` : 'v1/ranges';
    const response = await this.send(url, path, 'GET', undefined, options?.signal);

    return {
      initialized: response['initialized'] === true,
      localEndpoint: stringOf(response['localEndpoint']),
      keySpaces: ((response['keySpaces'] as JsonRecord[] | null) ?? []).map((space) => ({
        keySpace: stringOf(space['keySpace']),
        routingMode: stringOf(space['routingMode']),
        descriptors: ((space['descriptors'] as JsonRecord[] | null) ?? []).map((descriptor) => ({
          startKey: stringOrNull(descriptor['startKey']),
          endKey: stringOrNull(descriptor['endKey']),
          partitionId: numberOf(descriptor['partitionId']),
          generation: numberOf(descriptor['generation']),
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
    const response = await this.send(
      url,
      'v1/ranges/split',
      'POST',
      { keySpace, splitKey },
      options?.signal,
    );

    if (!response['status']) {
      throw KahunaError.lock('SplitRange returned no outcome', 'errored');
    }

    return {
      success: response['success'] === true,
      status: stringOf(response['status']),
      determinate: response['determinate'] === true,
      newPartitionId: numberOf(response['newPartitionId']),
      newGeneration: numberOf(response['newGeneration']),
      leaderHint: stringOrNull(response['leaderHint']),
      reason: stringOrNull(response['reason']),
    };
  }

  async mergeRanges(url: string, options?: CallOptions): Promise<MergeRangesResult> {
    const response = await this.send(url, 'v1/ranges/merge', 'POST', {}, options?.signal);

    if (!response['status']) {
      throw KahunaError.lock('MergeRanges returned no outcome', 'errored');
    }

    return {
      success: response['success'] === true,
      status: stringOf(response['status']),
      determinate: response['determinate'] === true,
      merges: numberOf(response['merges']),
      leaderHint: stringOrNull(response['leaderHint']),
      reason: stringOrNull(response['reason']),
    };
  }

  // ── Cluster ────────────────────────────────────────────────────────────────

  async getRoutingMetadata(
    url: string,
    keySpace: string | null,
    options?: CallOptions,
  ): Promise<RoutingMetadata> {
    const path = `v1/cluster/routing?keySpace=${encodeURIComponent(keySpace ?? '')}`;
    const response = await this.send(url, path, 'GET', undefined, options?.signal);

    return {
      initialized: response['initialized'] === true,
      schemaVersion: numberOf(response['schemaVersion']),
      hashAlgorithm: stringOf(response['hashAlgorithm']),
      prefixSeparator: stringOf(response['prefixSeparator']),
      groupSeparator: stringOf(response['groupSeparator']),
      hashPoolSize: numberOf(response['hashPoolSize']),
      hashPartitionOffset: numberOf(response['hashPartitionOffset']),
      sequenceStorageKeyFormat: stringOf(response['sequenceStorageKeyFormat']),
      reservedKeyPrefix: stringOf(response['reservedKeyPrefix']),
      localEndpoint: stringOf(response['localEndpoint']),
      snapshotVersion: numberOf(response['snapshotVersion']),
      coherent: response['coherent'] === true,
      keySpaces: ((response['keySpaces'] as JsonRecord[] | null) ?? []).map((space) => ({
        keySpace: stringOf(space['keySpace']),
        routingMode: stringOf(space['routingMode']),
        ranges: ((space['ranges'] as JsonRecord[] | null) ?? []).map((range) => ({
          startKey: stringOrNull(range['startKey']),
          endKey: stringOrNull(range['endKey']),
          partitionId: numberOf(range['partitionId']),
          generation: numberOf(range['generation']),
        })),
      })),
      leaders: ((response['leaders'] as JsonRecord[] | null) ?? []).map((leader) => ({
        partitionId: numberOf(leader['partitionId']),
        endpoint: stringOf(leader['endpoint']),
      })),
    };
  }

  async getClusterMembership(url: string, options?: CallOptions): Promise<ClusterMembership> {
    const response = await this.send(
      url,
      'v1/cluster/membership',
      'GET',
      undefined,
      options?.signal,
    );

    return {
      membershipVersion: numberOf(response['membershipVersion']),
      members: ((response['members'] as JsonRecord[] | null) ?? []).map((member) => ({
        endpoint: stringOf(member['endpoint']),
        nodeId: numberOf(member['nodeId']),
        role: stringOf(member['role']),
        joinedVersion: numberOf(member['joinedVersion']),
      })),
      localRole: stringOf(response['localRole']),
      initialized: response['initialized'] === true,
    };
  }

  async getClusterPlacement(url: string, options?: CallOptions): Promise<ClusterPlacement> {
    const response = await this.send(
      url,
      'v1/cluster/placement',
      'GET',
      undefined,
      options?.signal,
    );

    return {
      replicationFactor: numberOf(response['replicationFactor']),
      rebalancerEnabled: response['rebalancerEnabled'] === true,
      initialized: response['initialized'] === true,
      localEndpoint: stringOf(response['localEndpoint']),
      hostedPartitionCount: numberOf(response['hostedPartitionCount']),
      partitions: ((response['partitions'] as JsonRecord[] | null) ?? []).map((partition) => ({
        partitionId: numberOf(partition['partitionId']),
        state: stringOf(partition['state']),
        generation: numberOf(partition['generation']),
        effectiveReplicationFactor: numberOf(partition['effectiveReplicationFactor']),
        hostedLocally: partition['hostedLocally'] === true,
        replicas: ((partition['replicas'] as JsonRecord[] | null) ?? []).map((replica) => ({
          endpoint: stringOf(replica['endpoint']),
          role: stringOf(replica['role']),
        })),
      })),
    };
  }

  async leaveCluster(url: string, options?: CallOptions): Promise<ClusterLeaveResult> {
    const response = await this.send(url, 'v1/cluster/leave', 'POST', {}, options?.signal);

    if (!response['outcome']) {
      throw KahunaError.lock('LeaveCluster returned no outcome', 'errored');
    }

    return {
      left: response['left'] === true,
      drained: response['drained'] === true,
      outcome: stringOf(response['outcome']),
      membershipVersion: numberOf(response['membershipVersion']),
      retryable: response['retryable'] === true,
      reason: stringOf(response['reason']),
    };
  }

  async setReplicationFactor(
    url: string,
    partitionId: number,
    replicationFactor: number,
    options?: CallOptions,
  ): Promise<SetReplicationFactorResult> {
    const response = await this.send(
      url,
      'v1/cluster/replication-factor',
      'POST',
      { partitionId, replicationFactor },
      options?.signal,
    );

    if (!response['status']) {
      throw KahunaError.lock('SetReplicationFactor returned no outcome', 'errored');
    }

    return {
      success: response['success'] === true,
      status: stringOf(response['status']),
      generation: numberOf(response['generation']),
      reason: stringOrNull(response['reason']),
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
    const response = await this.send(
      url,
      'v1/kv/snapshot-hold/acquire',
      'POST',
      { holderId, timestamp: hlcToJson(timestamp), leaseMs },
      options?.signal,
    );

    return {
      type: keyValueResponseFromCode(response['type'] as number),
      holdId: stringOf(response['holdId']),
      leaseExpiry: hlcFromJson(response['leaseExpiry'] as HlcJson | null),
    };
  }

  async renewSnapshotHold(
    url: string,
    holdId: string,
    leaseMs: number,
    options?: CallOptions,
  ): Promise<Omit<SnapshotHold, 'holdId'>> {
    const response = await this.send(
      url,
      'v1/kv/snapshot-hold/renew',
      'POST',
      { holdId, leaseMs },
      options?.signal,
    );

    return {
      type: keyValueResponseFromCode(response['type'] as number),
      leaseExpiry: hlcFromJson(response['leaseExpiry'] as HlcJson | null),
    };
  }

  async releaseSnapshotHold(
    url: string,
    holdId: string,
    options?: CallOptions,
  ): Promise<KeyValueResponseCode> {
    const response = await this.send(
      url,
      'v1/kv/snapshot-hold/release',
      'POST',
      { holdId },
      options?.signal,
    );

    return keyValueResponseFromCode(response['type'] as number);
  }

  async getSnapshotFloor(url: string, options?: CallOptions): Promise<SnapshotFloor> {
    const response = await this.send(url, 'v1/kv/snapshot-floor', 'GET', undefined, options?.signal);

    const type = keyValueResponseFromCode(response['type'] as number);
    if (type !== 'get' && type !== 'set') {
      throw KahunaError.keyValue('GetSnapshotFloor failed', type);
    }

    return {
      effectiveFloor: hlcFromJson(response['effectiveFloor'] as HlcJson | null),
      liveHolds: numberOf(response['liveHolds']),
    };
  }

  // ── Backups ────────────────────────────────────────────────────────────────

  async takeFullBackup(url: string, options?: CallOptions): Promise<BackupInfo> {
    return readBackupInfo(
      await this.backupCall(url, 'v1/backups/full', 'POST', {}, options?.signal),
    );
  }

  async takeIncrementalBackup(
    url: string,
    parentBackupId: string,
    options?: CallOptions,
  ): Promise<BackupInfo> {
    return readBackupInfo(
      await this.backupCall(
        url,
        'v1/backups/incremental',
        'POST',
        { parentBackupId },
        options?.signal,
      ),
    );
  }

  async takeCoordinatedBackup(url: string, options?: CallOptions): Promise<BackupInfo> {
    return readBackupInfo(
      await this.backupCall(url, 'v1/backups/coordinated', 'POST', {}, options?.signal),
    );
  }

  async listBackups(url: string, options?: CallOptions): Promise<BackupInfo[]> {
    const response = await this.backupCall(url, 'v1/backups', 'GET', undefined, options?.signal);
    return asArray(response).map(readBackupInfo);
  }

  async getBackupChain(
    url: string,
    leafBackupId: string,
    options?: CallOptions,
  ): Promise<BackupInfo[]> {
    const response = await this.backupCall(
      url,
      `v1/backups/${encodeURIComponent(leafBackupId)}/chain`,
      'GET',
      undefined,
      options?.signal,
    );
    return asArray(response).map(readBackupInfo);
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
      'v1/restore',
      'POST',
      { leafBackupId, targetDir, targetTimeMs },
      options?.signal,
    );

    return {
      targetDir: stringOf(response['targetDir']),
      partitionsRestored: numberOf(response['partitionsRestored']),
      entriesApplied: numberOf(response['entriesApplied']),
      lastAppliedPhysicalMs: numberOf(response['lastAppliedPhysicalMs']),
      chain: ((response['chain'] as JsonRecord[] | null) ?? []).map(readBackupInfo),
      outcome: stringOf(response['outcome']),
      minRecoverablePhysicalMs: numberOf(response['minRecoverablePhysicalMs']),
      maxRecoverablePhysicalMs: numberOf(response['maxRecoverablePhysicalMs']),
    };
  }

  async collectBackupGarbage(
    url: string,
    dryRun: boolean,
    options?: CallOptions,
  ): Promise<BackupGcResult> {
    const response = await this.backupCall(
      url,
      `v1/backups/gc?dryRun=${dryRun}`,
      'POST',
      {},
      options?.signal,
    );

    return {
      applied: response['applied'] === true,
      bytesReclaimed: numberOf(response['bytesReclaimed']),
      retentionDeletions: ((response['retentionDeletions'] as JsonRecord[] | null) ?? []).map(
        (row) => ({
          backupId: stringOf(row['backupId']),
          type: stringOf(row['type']),
          createdAtUtc: stringOf(row['createdAtUtc']),
          bytes: numberOf(row['bytes']),
          reason: stringOf(row['reason']),
        }),
      ),
      orphanReclamations: ((response['orphanReclamations'] as JsonRecord[] | null) ?? []).map(
        (row) => ({
          name: stringOf(row['name']),
          isDirectory: row['isDirectory'] === true,
          reason: stringOf(row['reason']),
        }),
      ),
    };
  }

  /**
   * Runs a backup call and turns the typed refusal header into a typed error.
   *
   * Without it every refusal would read as a bare HTTP status, and an operator could
   * not tell "no backup directory configured" from "the chain is corrupt".
   */
  private async backupCall(
    url: string,
    path: string,
    method: 'GET' | 'POST',
    body: unknown,
    signal: AbortSignal | undefined,
  ): Promise<JsonRecord> {
    try {
      return await this.send(url, path, method, body, signal);
    } catch (error) {
      if (error instanceof HttpStatusError) {
        const outcome = readBackupOutcomeHeader(error);
        if (outcome !== null) throw new KahunaBackupError(outcome, error.message, { cause: error });
      }
      throw error;
    }
  }
}

// ── Parsing helpers ──────────────────────────────────────────────────────────

function joinUrl(base: string, path: string): string {
  return `${base.replace(/\/+$/, '')}/${path}`;
}

function numberOf(value: unknown): number {
  return typeof value === 'number' ? value : Number(value ?? 0);
}

function stringOf(value: unknown): string {
  return typeof value === 'string' ? value : '';
}

function stringOrNull(value: unknown): string | null {
  return typeof value === 'string' ? value : null;
}

function asArray(value: unknown): JsonRecord[] {
  return Array.isArray(value) ? (value as JsonRecord[]) : [];
}

function normalizeValue(value: string | Uint8Array | null): Uint8Array | null {
  if (value === null) return null;
  return typeof value === 'string' ? new TextEncoder().encode(value) : value;
}

function transactionFields(options: TransactionalCallOptions | undefined): JsonRecord {
  const transaction = options?.transaction;
  return {
    transactionId: hlcToJson(transaction?.transactionId ?? HLC_ZERO),
    coordinatorKey: transaction?.coordinatorKey ?? '',
    operationIdHigh: (transaction?.operationId ?? NO_OPERATION_ID).high,
    operationIdLow: (transaction?.operationId ?? NO_OPERATION_ID).low,
  };
}

function rangeFields(bounds: RangeBounds): JsonRecord {
  return {
    prefix: bounds.prefix,
    startKey: bounds.startKey ?? null,
    startInclusive: bounds.startInclusive ?? true,
    endKey: bounds.endKey ?? null,
    endInclusive: bounds.endInclusive ?? false,
  };
}

function readRouteHint(value: unknown): RouteHint | null {
  if (value === null || typeof value !== 'object') return null;
  const hint = value as JsonRecord;
  return {
    partitionId: numberOf(hint['partitionId']),
    endpoint: stringOf(hint['endpoint']),
    provenance: routeProvenanceFromCode(hint['provenance'] as number),
    generation: numberOf(hint['generation']),
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

function readScanItems(value: unknown): ScanItem[] {
  return asArray(value).map((item) => ({
    key: stringOf(item['key']),
    value: payloadFromJson(item['value'] as string | null),
    revision: numberOf(item['revision']),
    lastModified: hlcFromJson(item['lastModified'] as HlcJson | null),
  }));
}

function readBucketItems(response: JsonRecord, prefixKey: string): ScanItem[] {
  const type = keyValueResponseFromCode(response['type'] as number);
  if (type === 'get') return readScanItems(response['items']);
  if (type === 'doesNotExist') return [];
  throw KahunaError.keyValue(`Failed to scan key/values for '${prefixKey}': ${type}.`, type);
}

function readScriptValues(value: unknown): ScriptResult['values'] {
  return asArray(value).map((item) => ({
    key: stringOf(item['key']),
    value: payloadFromJson(item['value'] as string | null),
    revision: numberOf(item['revision']),
    expires: hlcFromJson(item['expires'] as HlcJson | null),
    lastModified: hlcFromJson(item['lastModified'] as HlcJson | null),
  }));
}

function readSequenceEntry(value: unknown): SequenceEntry | null {
  if (value === null || typeof value !== 'object') return null;
  const entry = value as JsonRecord;
  const maxValue = entry['maxValue'];
  const blockSize = entry['blockSize'];

  return {
    name: stringOf(entry['name']),
    currentValue: numberOf(entry['currentValue']),
    initialValue: numberOf(entry['initialValue']),
    increment: numberOf(entry['increment']),
    maxValue: maxValue === null || maxValue === undefined ? null : numberOf(maxValue),
    blockSize: blockSize === null || blockSize === undefined ? null : numberOf(blockSize),
    incarnation: numberOf(entry['incarnation']),
    revision: numberOf(entry['revision']),
    durability: 'persistent',
    createdAt: hlcFromJson(entry['createdAt'] as HlcJson | null),
    updatedAt: hlcFromJson(entry['updatedAt'] as HlcJson | null),
  };
}

function readBackupInfo(value: JsonRecord): BackupInfo {
  const nullableNumber = (field: unknown): number | null =>
    field === null || field === undefined ? null : numberOf(field);

  return {
    backupId: stringOf(value['backupId']),
    formatVersion: numberOf(value['formatVersion']),
    type: stringOf(value['type']),
    createdAtUtc: stringOf(value['createdAtUtc']),
    parentBackupId: stringOrNull(value['parentBackupId']),
    partitionCount: numberOf(value['partitionCount']),
    clusterId: stringOrNull(value['clusterId']),
    coordinatorNode: stringOrNull(value['coordinatorNode']),
    requestedKind: stringOrNull(value['requestedKind']),
    actualKind: stringOrNull(value['actualKind']),
    substitutionReason: stringOrNull(value['substitutionReason']),
    isInvalid: value['isInvalid'] === true,
    isIncomplete: value['isIncomplete'] === true,
    invalidReason: stringOrNull(value['invalidReason']),
    minRecoverablePhysicalMs: nullableNumber(value['minRecoverablePhysicalMs']),
    maxRecoverablePhysicalMs: nullableNumber(value['maxRecoverablePhysicalMs']),
  };
}

function readBackupOutcomeHeader(error: HttpStatusError): string | null {
  const header = error.header(BACKUP_OUTCOME_HEADER);
  if (header !== null) return header;

  // Some refusals carry the outcome in the body instead of the header.
  try {
    const parsed = JSON.parse(error.body) as JsonRecord;
    const outcome = parsed['outcome'];
    return typeof outcome === 'string' ? outcome : null;
  } catch {
    return null;
  }
}

export { BACKUP_OUTCOME_HEADER };
