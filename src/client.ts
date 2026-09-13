import {
  SET_MODE_FLAGS,
  type Durability,
  type RoutingDomain,
  type RoutingMode,
  type SetMode,
  type TransactionPriority,
} from './enums.js';
import { KahunaError, isKeyValueCode } from './errors.js';
import { HLC_ZERO, snapshotAt, type HlcTimestamp } from './hlc.js';
import { KeyValueEntry, type EntryOwner } from './entry.js';
import { KahunaLock, type LockOwner } from './lock.js';
import { resolveOptions, type KahunaClientOptions, type ResolvedOptions } from './options.js';
import { ClientRouteResolver, resolveRoutingMode } from './routing/resolver.js';
import { RouteCache } from './routing/route-cache.js';
import { RoutingEndpointPolicy } from './routing/endpoint-policy.js';
import { TransactionScript, type ScriptRunner } from './transaction-script.js';
import { TransactionSession } from './transaction-session.js';
import { newLockOwner, textToBytes, toBytes } from './transport/codec.js';
import { GrpcTransport } from './transport/grpc.js';
import { RestTransport } from './transport/rest.js';
import { decorrelatedJitterBackoff, delay } from './transport/retry.js';
import type { CallOptions, Transport } from './transport/transport.js';
import type {
  BackupGcResult,
  BackupInfo,
  ClusterLeaveResult,
  ClusterMembership,
  ClusterPlacement,
  DeleteManyItem,
  ExtendOutcome,
  GetManyItem,
  LockInfo,
  MergeRangesResult,
  RangeBounds,
  RangeMap,
  RegisterKeyRangeResult,
  RemoveKeyRangeResult,
  RestoreResult,
  ScriptParameter,
  ScriptResult,
  SequenceEntry,
  SequenceRange,
  SequenceUpdate,
  SetManyItem,
  SetReplicationFactorResult,
  SnapshotFloor,
  SnapshotHold,
  SplitRangeResult,
  TransactionOptions,
  ValueInput,
} from './types.js';

/** Options every read of a single key accepts. */
export interface ReadOptions {
  readonly durability?: Durability;
  /** Reads the value as of this wall-clock millisecond. Zero reads the latest. */
  readonly snapshotMs?: number;
  readonly signal?: AbortSignal;
}

/** Options every write of a single key accepts. */
export interface WriteOptions {
  readonly durability?: Durability;
  /** Milliseconds until the entry expires. Zero means it never expires. */
  readonly expiry?: number;
  readonly signal?: AbortSignal;
}

/** Options a `set` accepts. */
export interface SetOptions extends WriteOptions {
  /** The condition the server must find before it applies the write. */
  readonly mode?: SetMode;
}

/** Options acquiring a lock accepts. */
export interface AcquireLockOptions {
  /** Milliseconds the lock is held before it expires. */
  readonly expiry?: number;
  /** Milliseconds to keep trying when the lock is held. Zero gives up at once. */
  readonly wait?: number;
  /** Milliseconds between attempts. Required whenever `wait` is not zero. */
  readonly retry?: number;
  readonly durability?: Durability;
  readonly signal?: AbortSignal;
}

/** Options a range read accepts. */
export interface RangeOptions extends ReadOptions {
  readonly limit?: number;
}

/** Options a script run accepts. */
export interface ExecuteScriptOptions {
  readonly hash?: string | null;
  readonly parameters?: readonly ScriptParameter[] | null;
  readonly priority?: TransactionPriority;
  readonly signal?: AbortSignal;
}

const MAX_RETRYABLE_TRANSACTION_BACKOFF_MS = 500;

/**
 * The entry point of the Kahuna client.
 *
 * One client owns one transport and its connection pool. Create it once and share
 * it; creating one per operation opens a fresh pool every time.
 */
export class KahunaClient implements EntryOwner, LockOwner, ScriptRunner, AsyncDisposable {
  readonly #options: ResolvedOptions;

  readonly #transport: Transport;

  readonly #ownsTransport: boolean;

  readonly #router: ClientRouteResolver | null;

  readonly #effectiveRouting: RoutingMode;

  #nextEndpoint = 0;

  constructor(options: KahunaClientOptions) {
    this.#options = resolveOptions(options);

    const supplied = options.transport;
    if (typeof supplied === 'object') {
      this.#transport = supplied;
      this.#ownsTransport = false;
    } else {
      this.#transport = buildTransport(supplied ?? 'grpc', this.#options);
      this.#ownsTransport = true;
    }

    this.#effectiveRouting = resolveRoutingMode(
      this.#options.routing,
      this.#options.endpoints.length,
    );
    this.#router = this.buildRouter();
  }

  /** The routing mode in force, with `auto` already resolved. */
  get routing(): RoutingMode {
    return this.#effectiveRouting;
  }

  /** The route resolver, or null when the client rotates endpoints. */
  get router(): ClientRouteResolver | null {
    return this.#router;
  }

  /** The transport this client talks through. */
  get transport(): Transport {
    return this.#transport;
  }

  /** The endpoints this client was given. */
  get endpoints(): readonly string[] {
    return this.#options.endpoints;
  }

  /** Releases every connection. A client this closed cannot be reused. */
  async close(): Promise<void> {
    if (this.#ownsTransport) await this.#transport.close();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  // ── Locks ──────────────────────────────────────────────────────────────────

  /**
   * Takes a lock on a resource.
   *
   * With no `wait`, one attempt is made and a held lock comes back as a handle
   * whose `acquired` is false. With a `wait`, the client keeps trying for that long
   * and `retry` says how often.
   */
  async acquireLock(resource: string, options?: AcquireLockOptions): Promise<KahunaLock> {
    const expiry = options?.expiry ?? 30_000;
    const wait = options?.wait ?? 0;
    const durability = options?.durability ?? 'persistent';

    if (wait === 0) {
      return this.singleAttemptLock(resource, expiry, durability, options?.signal);
    }

    const retry = options?.retry ?? 0;
    if (retry === 0) throw KahunaError.lock('Retry cannot be zero', 'invalidInput');

    return this.repeatedlyAcquireLock(resource, expiry, wait, retry, durability, options?.signal);
  }

  /** Pushes a lock's expiry out. The owner token must be the one that holds it. */
  extendLock(
    resource: string,
    owner: Uint8Array | string,
    expiryMs: number,
    options?: { durability?: Durability; signal?: AbortSignal },
  ): Promise<ExtendOutcome> {
    const durability = options?.durability ?? 'persistent';
    return this.#transport.extendLock(
      this.urlFor('lock', resource),
      resource,
      asOwner(owner),
      expiryMs,
      durability,
      { signal: options?.signal },
    );
  }

  /** Releases a lock. Returns false when no such lock exists. */
  releaseLock(
    resource: string,
    owner: Uint8Array | string,
    options?: { durability?: Durability; signal?: AbortSignal },
  ): Promise<boolean> {
    const durability = options?.durability ?? 'persistent';
    return this.#transport.releaseLock(
      this.urlFor('lock', resource),
      resource,
      asOwner(owner),
      durability,
      { signal: options?.signal },
    );
  }

  /** Reads what the server holds for a lock. */
  getLockInfo(
    resource: string,
    options?: { durability?: Durability; signal?: AbortSignal },
  ): Promise<LockInfo | null> {
    const durability = options?.durability ?? 'persistent';
    return this.#transport.getLock(this.urlFor('lock', resource), resource, durability, {
      signal: options?.signal,
    });
  }

  // ── Key/value point operations ────────────────────────────────────────────

  /** Writes a value. An existing value is overwritten unless `mode` says otherwise. */
  async set(key: string, value: ValueInput, options?: SetOptions): Promise<KeyValueEntry> {
    const durability = options?.durability ?? 'persistent';
    const bytes = toBytes(value);

    const outcome = await this.#transport.set(
      this.urlFor('keyValue', key),
      key,
      bytes,
      options?.expiry ?? 0,
      SET_MODE_FLAGS[options?.mode ?? 'always'],
      durability,
      { signal: options?.signal },
    );

    return new KeyValueEntry(this, {
      key,
      success: outcome.success,
      value: bytes,
      revision: outcome.revision,
      durability,
      timeElapsedMs: outcome.timeElapsedMs,
    });
  }

  /**
   * Writes a value without archiving a historical revision entry.
   *
   * The revision counter still advances and conditional writes keep their meaning,
   * but a by-revision or snapshot read of this write finds nothing. Intended for
   * cache keys, where revision history is wasted work.
   */
  setNoRevision(
    key: string,
    value: ValueInput,
    options?: Omit<SetOptions, 'mode'>,
  ): Promise<KeyValueEntry> {
    return this.set(key, value, { ...options, mode: 'noRevision' });
  }

  /** Writes a value only when the key currently holds exactly `compareValue`. */
  async compareValueAndSet(
    key: string,
    value: ValueInput,
    compareValue: ValueInput,
    options?: WriteOptions,
  ): Promise<KeyValueEntry> {
    const durability = options?.durability ?? 'persistent';
    const bytes = toBytes(value);

    const outcome = await this.#transport.compareValueAndSet(
      this.urlFor('keyValue', key),
      key,
      bytes,
      toBytes(compareValue),
      options?.expiry ?? 0,
      durability,
      { signal: options?.signal },
    );

    return new KeyValueEntry(this, {
      key,
      success: outcome.success,
      value: bytes,
      revision: outcome.revision,
      durability,
      timeElapsedMs: outcome.timeElapsedMs,
    });
  }

  /** Writes a value only when the key is currently at exactly `compareRevision`. */
  async compareRevisionAndSet(
    key: string,
    value: ValueInput,
    compareRevision: number,
    options?: WriteOptions,
  ): Promise<KeyValueEntry> {
    const durability = options?.durability ?? 'persistent';
    const bytes = toBytes(value);

    const outcome = await this.#transport.compareRevisionAndSet(
      this.urlFor('keyValue', key),
      key,
      bytes,
      compareRevision,
      options?.expiry ?? 0,
      durability,
      { signal: options?.signal },
    );

    return new KeyValueEntry(this, {
      key,
      success: outcome.success,
      value: bytes,
      revision: outcome.revision,
      durability,
      timeElapsedMs: outcome.timeElapsedMs,
    });
  }

  /** Reads a key. A key that holds no value comes back with `success` false. */
  async get(key: string, options?: ReadOptions): Promise<KeyValueEntry> {
    const durability = options?.durability ?? 'persistent';
    const outcome = await this.#transport.get(this.urlFor('keyValue', key), key, -1, durability, {
      readTimestamp: snapshotAt(options?.snapshotMs ?? 0),
      signal: options?.signal,
    });

    return new KeyValueEntry(this, {
      key,
      success: outcome.success,
      value: outcome.value,
      revision: outcome.revision,
      durability,
      timeElapsedMs: outcome.timeElapsedMs,
      lastModified: outcome.lastModified.physical,
    });
  }

  /** Reads one archived revision of a key. */
  async getRevision(
    key: string,
    revision: number,
    options?: Omit<ReadOptions, 'snapshotMs'>,
  ): Promise<KeyValueEntry> {
    const durability = options?.durability ?? 'persistent';
    const outcome = await this.#transport.get(
      this.urlFor('keyValue', key),
      key,
      revision,
      durability,
      { signal: options?.signal },
    );

    return new KeyValueEntry(this, {
      key,
      success: outcome.success,
      value: outcome.value,
      revision: outcome.revision,
      durability,
      timeElapsedMs: outcome.timeElapsedMs,
      lastModified: outcome.lastModified.physical,
    });
  }

  /** Reports whether a key holds a value, without transferring that value. */
  async exists(key: string, options?: ReadOptions): Promise<KeyValueEntry> {
    const durability = options?.durability ?? 'persistent';
    const outcome = await this.#transport.exists(this.urlFor('keyValue', key), key, -1, durability, {
      readTimestamp: snapshotAt(options?.snapshotMs ?? 0),
      signal: options?.signal,
    });

    return new KeyValueEntry(this, {
      key,
      success: outcome.success,
      revision: outcome.revision,
      durability,
      timeElapsedMs: outcome.timeElapsedMs,
    });
  }

  /** Deletes a key. Returns `success` false when no such key exists. */
  async delete(
    key: string,
    options?: { durability?: Durability; signal?: AbortSignal },
  ): Promise<KeyValueEntry> {
    const durability = options?.durability ?? 'persistent';
    const outcome = await this.#transport.delete(this.urlFor('keyValue', key), key, durability, {
      signal: options?.signal,
    });

    return new KeyValueEntry(this, {
      key,
      success: outcome.success,
      revision: outcome.revision,
      durability,
      timeElapsedMs: outcome.timeElapsedMs,
    });
  }

  /** Pushes a key's expiry out by the given number of milliseconds. */
  async extend(
    key: string,
    expiryMs: number,
    options?: { durability?: Durability; signal?: AbortSignal },
  ): Promise<KeyValueEntry> {
    const durability = options?.durability ?? 'persistent';
    const outcome = await this.#transport.extend(
      this.urlFor('keyValue', key),
      key,
      expiryMs,
      durability,
      { signal: options?.signal },
    );

    return new KeyValueEntry(this, {
      key,
      success: outcome.success,
      revision: outcome.revision,
      durability,
      timeElapsedMs: outcome.timeElapsedMs,
    });
  }

  // ── Key/value batches ──────────────────────────────────────────────────────

  /** Writes several keys in one request. */
  async setMany(
    items: readonly SetManyItem[],
    options?: CallOptions,
  ): Promise<KeyValueEntry[]> {
    const outcome = await this.#transport.setMany(this.nextEndpoint(), items, options);

    return outcome.items.map(
      (item) =>
        new KeyValueEntry(this, {
          key: item.key,
          success: item.type === 'set',
          revision: item.revision,
          durability: item.durability,
          timeElapsedMs: outcome.timeElapsedMs,
        }),
    );
  }

  /** Deletes several keys in one request. */
  async deleteMany(
    keys: readonly string[] | readonly DeleteManyItem[],
    options?: { durability?: Durability; signal?: AbortSignal },
  ): Promise<KeyValueEntry[]> {
    const defaultDurability = options?.durability ?? 'persistent';
    const items: DeleteManyItem[] = keys.map((entry) =>
      typeof entry === 'string'
        ? { key: entry, durability: defaultDurability }
        : { key: entry.key, durability: entry.durability ?? defaultDurability },
    );

    const outcome = await this.#transport.deleteMany(this.nextEndpoint(), items, {
      signal: options?.signal,
    });

    return outcome.items.map(
      (item) =>
        new KeyValueEntry(this, {
          key: item.key,
          success: item.type === 'deleted',
          revision: item.revision,
          durability: item.durability,
          timeElapsedMs: outcome.timeElapsedMs,
        }),
    );
  }

  /** Reads several keys in one request. */
  async getMany(items: readonly GetManyItem[], options?: CallOptions): Promise<KeyValueEntry[]> {
    const outcome = await this.#transport.getMany(this.nextEndpoint(), items, options);
    return outcome.items.map(
      (item) =>
        new KeyValueEntry(this, {
          key: item.key,
          success: item.type === 'get',
          value: item.value,
          revision: item.revision,
          durability: item.durability,
          timeElapsedMs: outcome.timeElapsedMs,
          lastModified: item.lastModified.physical,
        }),
    );
  }

  /** Reports for several keys whether each holds a value. */
  async existsMany(items: readonly GetManyItem[], options?: CallOptions): Promise<KeyValueEntry[]> {
    const outcome = await this.#transport.existsMany(this.nextEndpoint(), items, options);
    return outcome.items.map(
      (item) =>
        new KeyValueEntry(this, {
          key: item.key,
          success: item.type === 'exists',
          value: item.value,
          revision: item.revision,
          durability: item.durability,
          timeElapsedMs: outcome.timeElapsedMs,
          lastModified: item.lastModified.physical,
        }),
    );
  }

  // ── Scans ──────────────────────────────────────────────────────────────────

  /** Reads every key in one bucket: the prefix up to, and not including, the last `/`. */
  async getByBucket(prefixKey: string, options?: ReadOptions): Promise<KeyValueEntry[]> {
    const durability = options?.durability ?? 'persistent';
    const items = await this.#transport.getByBucket(this.nextEndpoint(), prefixKey, durability, {
      readTimestamp: snapshotAt(options?.snapshotMs ?? 0),
      signal: options?.signal,
    });

    return items.map((item) => this.entryFromScan(item, durability));
  }

  /** Reads every key whose name starts with this prefix, across every bucket. */
  async scanAllByPrefix(prefixKey: string, options?: ReadOptions): Promise<KeyValueEntry[]> {
    const durability = options?.durability ?? 'persistent';
    const items = await this.#transport.scanAllByPrefix(this.nextEndpoint(), prefixKey, durability, {
      readTimestamp: snapshotAt(options?.snapshotMs ?? 0),
      signal: options?.signal,
    });

    return items.map((item) => this.entryFromScan(item, durability));
  }

  /** Reads one page of an ordered key range. */
  async getByRange(bounds: RangeBounds, options?: RangeOptions): Promise<KeyValueEntry[]> {
    const durability = options?.durability ?? 'persistent';
    const page = await this.#transport.getByRange(
      this.nextEndpoint(),
      bounds,
      options?.limit ?? 100,
      durability,
      { readTimestamp: snapshotAt(options?.snapshotMs ?? 0), signal: options?.signal },
    );

    return page.items.map((item) => this.entryFromScan(item, durability));
  }

  /** Walks an ordered key range page by page. */
  async *scanByRange(
    bounds: RangeBounds,
    options?: RangeOptions,
  ): AsyncIterable<KeyValueEntry> {
    const durability = options?.durability ?? 'persistent';
    const items = this.#transport.scanByRange(
      this.nextEndpoint(),
      bounds,
      options?.limit ?? 100,
      durability,
      { readTimestamp: snapshotAt(options?.snapshotMs ?? 0), signal: options?.signal },
    );

    for await (const item of items) yield this.entryFromScan(item, durability);
  }

  // ── Transactions ───────────────────────────────────────────────────────────

  /** Parses a script once, so later runs reuse the server's parsed form. */
  loadScript(script: string | Uint8Array): TransactionScript {
    return new TransactionScript(this, script);
  }

  /** Runs a transaction script. */
  executeScript(
    script: string | Uint8Array,
    options?: ExecuteScriptOptions,
  ): Promise<ScriptResult> {
    const bytes = typeof script === 'string' ? textToBytes(script) : script;
    return this.#transport.executeScript(
      this.nextEndpoint(),
      bytes,
      options?.hash ?? null,
      options?.parameters ?? null,
      options?.priority ?? 'normal',
      { signal: options?.signal },
    );
  }

  /** Opens a transaction session on one node. */
  async beginTransaction(
    options: TransactionOptions = {},
    call?: CallOptions,
  ): Promise<TransactionSession> {
    const coordinatorKey = crypto.randomUUID();
    const started = await this.#transport.startTransaction(
      this.nextEndpoint(),
      coordinatorKey,
      options,
      call,
    );

    return new TransactionSession(
      this.#transport,
      this,
      started.url,
      coordinatorKey,
      started.transactionId,
      options.locking ?? 'pessimistic',
      options.timeout ?? 0,
      options.readTimestamp ?? HLC_ZERO,
    );
  }

  /**
   * Runs a transaction, starting a fresh one on every retryable conflict.
   *
   * The callback receives an open session and must finalize it. A conflict, a
   * transient refusal or a lock it could not take starts the next attempt after a
   * back-off; anything else is raised to the caller.
   */
  async withTransaction<T>(
    options: TransactionOptions,
    body: (session: TransactionSession, signal?: AbortSignal) => Promise<T>,
    call?: CallOptions,
  ): Promise<T> {
    for (const backoff of decorrelatedJitterBackoff(50, 10)) {
      const session = await this.beginTransaction(options, call);

      try {
        return await body(session, call?.signal);
      } catch (error) {
        if (
          isKeyValueCode(error, 'aborted') ||
          isKeyValueCode(error, 'mustRetry') ||
          isKeyValueCode(error, 'alreadyLocked')
        ) {
          await delay(Math.min(backoff, MAX_RETRYABLE_TRANSACTION_BACKOFF_MS), call?.signal);
          continue;
        }
        throw error;
      } finally {
        await session[Symbol.asyncDispose]();
      }
    }

    throw KahunaError.keyValue('Transaction aborted', 'aborted');
  }

  // ── Sequences ──────────────────────────────────────────────────────────────

  /** Creates a sequence and returns it as the server now holds it. */
  async createSequence(
    name: string,
    options?: {
      initialValue?: number;
      increment?: number;
      maxValue?: number | null;
      /**
       * Values this sequence reserves per commit. Omit it to use the server-wide
       * setting. `1` is gap-free at one commit per value.
       */
      blockSize?: number | null;
      signal?: AbortSignal;
    },
  ): Promise<SequenceEntry> {
    const outcome = await this.#transport.createSequence(
      this.urlFor('sequence', name),
      name,
      options?.initialValue ?? 0,
      options?.increment ?? 1,
      options?.maxValue ?? null,
      options?.blockSize ?? null,
      { signal: options?.signal },
    );

    if (outcome.type !== 'success') {
      throw KahunaError.sequence(`Failed to create sequence: ${outcome.type}`, outcome.type);
    }

    const created = await this.getSequence(name, { signal: options?.signal });
    if (created === null) {
      throw KahunaError.sequence('Created sequence could not be read', 'error');
    }
    return created;
  }

  /**
   * Rewrites a sequence's parameters, usually its current value, and starts a new
   * incarnation of its value stream. This is the `setval` / `ALTER SEQUENCE RESTART`
   * operation. It returns the sequence as it reads after the update.
   *
   * The call takes about one server block lease to return (`SequencerBlockLease`,
   * 5 s by default). This is by design: the server withholds success until no node
   * can still issue values from a block of the old incarnation. For the same
   * interval, the sequence refuses allocations with `mustRetry`. Give the call a
   * deadline longer than the lease, or a correct update reads as a timeout.
   *
   * A lower current value makes the sequence issue values it issued before. The
   * caller decides whether values from two incarnations may overlap.
   */
  async updateSequence(
    name: string,
    update: SequenceUpdate,
    options?: CallOptions,
  ): Promise<SequenceEntry> {
    const outcome = await this.#transport.updateSequence(
      this.urlFor('sequence', name),
      name,
      update,
      options,
    );

    if (outcome.type !== 'success') {
      throw KahunaError.sequence(`Failed to update sequence: ${outcome.type}`, outcome.type);
    }

    const updated = await this.getSequence(name, options);
    if (updated === null) {
      throw KahunaError.sequence('Updated sequence could not be read', 'error');
    }
    return updated;
  }

  /** Reads a sequence, or null when there is none by that name. */
  async getSequence(name: string, options?: CallOptions): Promise<SequenceEntry | null> {
    const outcome = await this.#transport.getSequence(
      this.urlFor('sequence', name),
      name,
      options,
    );

    if (outcome.type === 'notFound') return null;
    if (outcome.type !== 'success' || outcome.entry === null) {
      throw KahunaError.sequence(`Failed to get sequence: ${outcome.type}`, outcome.type);
    }
    return outcome.entry;
  }

  /**
   * Takes the next value of a sequence.
   *
   * Pass an `idempotencyKey` when a retry must not consume a second value.
   */
  async nextSequenceValue(
    name: string,
    options?: { idempotencyKey?: string | null; signal?: AbortSignal },
  ): Promise<number> {
    const outcome = await this.#transport.reserveSequenceRange(
      this.urlFor('sequence', name),
      name,
      1,
      options?.idempotencyKey ?? null,
      { signal: options?.signal },
    );

    if (outcome.type !== 'success') {
      throw KahunaError.sequence(
        `Failed to reserve sequence range: ${outcome.type}`,
        outcome.type,
      );
    }
    return outcome.allocation.start;
  }

  /** Reserves a block of sequence values for this caller alone. */
  async reserveSequenceRange(
    name: string,
    count: number,
    options?: { idempotencyKey?: string | null; signal?: AbortSignal },
  ): Promise<SequenceRange> {
    const outcome = await this.#transport.reserveSequenceRange(
      this.urlFor('sequence', name),
      name,
      count,
      options?.idempotencyKey ?? null,
      { signal: options?.signal },
    );

    if (outcome.type !== 'success') {
      throw KahunaError.sequence(
        `Failed to reserve sequence range: ${outcome.type}`,
        outcome.type,
      );
    }
    return outcome.allocation;
  }

  /** Deletes a sequence. Returns false when there was none by that name. */
  async deleteSequence(name: string, options?: CallOptions): Promise<boolean> {
    const outcome = await this.#transport.deleteSequence(
      this.urlFor('sequence', name),
      name,
      options,
    );

    if (outcome.type === 'notFound') return false;
    if (outcome.type !== 'success') {
      throw KahunaError.sequence(`Failed to delete sequence: ${outcome.type}`, outcome.type);
    }
    return true;
  }

  // ── Key ranges ─────────────────────────────────────────────────────────────

  /**
   * Puts a key space under key-range routing on one node, and seeds its descriptor.
   *
   * The routing half is node-local and unreplicated, so this must be sent to every
   * node. Only the seed descriptor is replicated.
   */
  registerKeyRange(
    keySpace: string,
    options?: { nodeUrl?: string; signal?: AbortSignal },
  ): Promise<RegisterKeyRangeResult> {
    return this.#transport.registerKeyRange(this.nodeUrl(options?.nodeUrl), keySpace, {
      signal: options?.signal,
    });
  }

  /** Drops a key space's descriptors from the replicated range map. */
  removeKeyRange(
    keySpace: string,
    options?: { nodeUrl?: string; signal?: AbortSignal },
  ): Promise<RemoveKeyRangeResult> {
    return this.#transport.removeKeyRange(this.nodeUrl(options?.nodeUrl), keySpace, {
      signal: options?.signal,
    });
  }

  /** Reads the range-descriptor map as one node has applied it. */
  getRanges(options?: {
    keySpace?: string;
    nodeUrl?: string;
    signal?: AbortSignal;
  }): Promise<RangeMap> {
    return this.#transport.getRanges(this.nodeUrl(options?.nodeUrl), options?.keySpace ?? null, {
      signal: options?.signal,
    });
  }

  /** Splits the range covering a key at exactly that key. */
  splitRange(
    keySpace: string,
    splitKey: string,
    options?: { nodeUrl?: string; signal?: AbortSignal },
  ): Promise<SplitRangeResult> {
    return this.#transport.splitRange(this.nodeUrl(options?.nodeUrl), keySpace, splitKey, {
      signal: options?.signal,
    });
  }

  /** Runs the merge pass, folding adjacent under-sized ranges. */
  mergeRanges(options?: { nodeUrl?: string; signal?: AbortSignal }): Promise<MergeRangesResult> {
    return this.#transport.mergeRanges(this.nodeUrl(options?.nodeUrl), {
      signal: options?.signal,
    });
  }

  // ── Cluster ────────────────────────────────────────────────────────────────

  getClusterMembership(options?: CallOptions): Promise<ClusterMembership> {
    return this.#transport.getClusterMembership(this.nextEndpoint(), options);
  }

  getClusterPlacement(options?: {
    nodeUrl?: string;
    signal?: AbortSignal;
  }): Promise<ClusterPlacement> {
    return this.#transport.getClusterPlacement(this.nodeUrl(options?.nodeUrl), {
      signal: options?.signal,
    });
  }

  /**
   * Asks one node to leave the cluster roster.
   *
   * A client with several endpoints must name the node, because there is no sensible
   * default for which one to decommission.
   */
  leaveCluster(options?: { nodeUrl?: string; signal?: AbortSignal }): Promise<ClusterLeaveResult> {
    if (!options?.nodeUrl && this.#options.endpoints.length > 1) {
      throw new TypeError(
        'leaveCluster requires the endpoint of the node to decommission when the client is configured with multiple endpoints.',
      );
    }

    return this.#transport.leaveCluster(options?.nodeUrl ?? this.#options.endpoints[0]!, {
      signal: options?.signal,
    });
  }

  setReplicationFactor(
    partitionId: number,
    replicationFactor: number,
    options?: { nodeUrl?: string; signal?: AbortSignal },
  ): Promise<SetReplicationFactorResult> {
    return this.#transport.setReplicationFactor(
      this.nodeUrl(options?.nodeUrl),
      partitionId,
      replicationFactor,
      { signal: options?.signal },
    );
  }

  // ── Snapshot holds ─────────────────────────────────────────────────────────

  /** Pins the MVCC snapshot floor at a timestamp for the length of a lease. */
  acquireSnapshotHold(
    holderId: string,
    timestamp: HlcTimestamp,
    leaseMs: number,
    options?: CallOptions,
  ): Promise<SnapshotHold> {
    return this.#transport.acquireSnapshotHold(
      this.nextEndpoint(),
      holderId,
      timestamp,
      leaseMs,
      options,
    );
  }

  renewSnapshotHold(
    holdId: string,
    leaseMs: number,
    options?: CallOptions,
  ): Promise<Omit<SnapshotHold, 'holdId'>> {
    return this.#transport.renewSnapshotHold(this.nextEndpoint(), holdId, leaseMs, options);
  }

  releaseSnapshotHold(holdId: string, options?: CallOptions) {
    return this.#transport.releaseSnapshotHold(this.nextEndpoint(), holdId, options);
  }

  getSnapshotFloor(options?: CallOptions): Promise<SnapshotFloor> {
    return this.#transport.getSnapshotFloor(this.nextEndpoint(), options);
  }

  // ── Backups ────────────────────────────────────────────────────────────────

  takeFullBackup(options?: CallOptions): Promise<BackupInfo> {
    return this.#transport.takeFullBackup(this.nextEndpoint(), options);
  }

  takeIncrementalBackup(parentBackupId: string, options?: CallOptions): Promise<BackupInfo> {
    return this.#transport.takeIncrementalBackup(this.nextEndpoint(), parentBackupId, options);
  }

  takeCoordinatedBackup(options?: CallOptions): Promise<BackupInfo> {
    return this.#transport.takeCoordinatedBackup(this.nextEndpoint(), options);
  }

  listBackups(options?: CallOptions): Promise<BackupInfo[]> {
    return this.#transport.listBackups(this.nextEndpoint(), options);
  }

  getBackupChain(leafBackupId: string, options?: CallOptions): Promise<BackupInfo[]> {
    return this.#transport.getBackupChain(this.nextEndpoint(), leafBackupId, options);
  }

  restore(
    leafBackupId: string,
    targetDir: string,
    options?: { targetTimeMs?: number; signal?: AbortSignal },
  ): Promise<RestoreResult> {
    return this.#transport.restore(
      this.nextEndpoint(),
      leafBackupId,
      targetDir,
      options?.targetTimeMs ?? 0,
      { signal: options?.signal },
    );
  }

  collectBackupGarbage(
    options?: { dryRun?: boolean; signal?: AbortSignal },
  ): Promise<BackupGcResult> {
    return this.#transport.collectBackupGarbage(this.nextEndpoint(), options?.dryRun ?? false, {
      signal: options?.signal,
    });
  }

  // ── Handle call-backs ──────────────────────────────────────────────────────

  /** @internal Used by a lock handle to reach its own resource. */
  lockUrlFor(resource: string, servedFrom: string | null): string {
    if (!this.#options.upgradeUrls || !servedFrom) return this.urlFor('lock', resource);

    const resolver = this.#router;
    if (resolver === null) return servedFrom;

    return resolver.tryUseAffinity(servedFrom) ?? this.urlFor('lock', resource);
  }

  /** @internal */
  extendLockAt(
    url: string,
    resource: string,
    owner: Uint8Array,
    expiryMs: number,
    durability: Durability,
    signal?: AbortSignal,
  ): Promise<ExtendOutcome> {
    return this.#transport.extendLock(url, resource, owner, expiryMs, durability, { signal });
  }

  /** @internal */
  releaseLockAt(
    url: string,
    resource: string,
    owner: Uint8Array,
    durability: Durability,
    signal?: AbortSignal,
  ): Promise<boolean> {
    return this.#transport.releaseLock(url, resource, owner, durability, { signal });
  }

  /** @internal */
  getLockAt(
    url: string,
    resource: string,
    durability: Durability,
    signal?: AbortSignal,
  ): Promise<LockInfo | null> {
    return this.#transport.getLock(url, resource, durability, { signal });
  }

  // ── Internals ──────────────────────────────────────────────────────────────

  private entryFromScan(
    item: { key: string; value: Uint8Array | null; revision: number; lastModified: HlcTimestamp },
    durability: Durability,
  ): KeyValueEntry {
    return new KeyValueEntry(this, {
      key: item.key,
      success: true,
      value: item.value,
      revision: item.revision,
      durability,
      timeElapsedMs: 0,
      lastModified: item.lastModified.physical,
    });
  }

  private buildRouter(): ClientRouteResolver | null {
    if (this.#effectiveRouting === 'roundRobin') return null;

    const resolver = new ClientRouteResolver(
      this.#effectiveRouting,
      new RouteCache(this.#options.routeCacheCapacity, this.#options.routeHintLifetimeMs),
      new RoutingEndpointPolicy(
        this.#options.endpoints,
        this.#options.routingEndpointMap,
        this.#options.allowUnlistedRoutingEndpoints,
      ),
      this.#transport,
      () => this.nextEndpoint(),
      this.#options.routingMetadataLifetimeMs,
      this.#options.routingEndpointCooldownMs,
    );

    this.#transport.routeSink = resolver;
    return resolver;
  }

  /**
   * The endpoint to send an operation on this resource to.
   *
   * A learned destination is an efficiency choice. Whichever node receives the
   * request resolves the resource itself, so a stale answer costs an inter-node
   * forward and cannot change the outcome.
   */
  private urlFor(domain: RoutingDomain, resource: string): string {
    const resolver = this.#router;
    if (resolver === null) return this.nextEndpoint();
    return resolver.select(domain, resource) ?? this.nextEndpoint();
  }

  private nodeUrl(explicit: string | undefined): string {
    return explicit && explicit.length > 0 ? explicit : this.nextEndpoint();
  }

  private nextEndpoint(): string {
    const endpoints = this.#options.endpoints;
    if (endpoints.length === 1) return endpoints[0]!;

    const index = this.#nextEndpoint++;
    return endpoints[Math.abs(index) % endpoints.length]!;
  }

  /** One attempt. A lock another holder owns comes back as an unacquired handle. */
  private async singleAttemptLock(
    resource: string,
    expiryMs: number,
    durability: Durability,
    signal: AbortSignal | undefined,
  ): Promise<KahunaLock> {
    const owner = newLockOwner();
    const acquisition = await this.#transport.acquireLock(
      this.urlFor('lock', resource),
      resource,
      owner,
      expiryMs,
      durability,
      { signal },
    );

    return new KahunaLock(
      this,
      resource,
      acquisition.result,
      owner,
      durability,
      acquisition.fencingToken,
      acquisition.servedFrom,
    );
  }

  /**
   * Retries until the wait budget runs out.
   *
   * The retry interval is jittered so a crowd of waiters does not converge on one
   * instant and collide on every attempt.
   */
  private async repeatedlyAcquireLock(
    resource: string,
    expiryMs: number,
    waitMs: number,
    retryMs: number,
    durability: Durability,
    signal: AbortSignal | undefined,
  ): Promise<KahunaLock> {
    const owner = newLockOwner();
    const deadline = Date.now() + waitMs;

    let fencingToken = -1;
    let servedFrom: string | null = null;
    let result: import('./types.js').LockAcquireResult = 'error';

    while (Date.now() < deadline) {
      const acquisition = await this.#transport.acquireLock(
        this.urlFor('lock', resource),
        resource,
        owner,
        expiryMs,
        durability,
        { signal },
      );

      result = acquisition.result;
      fencingToken = acquisition.fencingToken;
      servedFrom = acquisition.servedFrom;

      if (result === 'acquired') {
        return new KahunaLock(this, resource, result, owner, durability, fencingToken, servedFrom);
      }

      await delay(Math.max(100, retryMs + Math.floor(Math.random() * 100) - 50), signal);
    }

    // The budget ran out. The handle reports the last outcome and owns nothing, so
    // disposing it releases nothing.
    return new KahunaLock(this, resource, result, null, durability, fencingToken, servedFrom);
  }
}

function buildTransport(kind: 'grpc' | 'rest', options: ResolvedOptions): Transport {
  const security = {
    allowInsecureCertificateValidation: options.allowInsecureCertificateValidation,
    trustedServerCertificateThumbprints: options.trustedServerCertificateThumbprints,
  };

  if (kind === 'rest') {
    return new RestTransport({
      ...security,
      bearerToken: options.bearerToken,
      useHttp2: options.useHttp2,
    });
  }

  return new GrpcTransport({
    ...security,
    defaultOperationTimeoutMs: options.defaultOperationTimeoutMs,
  });
}

function asOwner(owner: Uint8Array | string): Uint8Array {
  return typeof owner === 'string' ? textToBytes(owner) : owner;
}

/** Re-exported so a caller can mint an owner token without acquiring a lock. */
export { newLockOwner };
