import type { Durability, RangeLockMode, SetMode, TransactionLocking } from './enums.js';
import { SET_MODE_FLAGS } from './enums.js';
import { KahunaError, isKeyValueCode } from './errors.js';
import { HLC_ZERO, type HlcTimestamp } from './hlc.js';
import { KeyValueEntry, type EntryOwner } from './entry.js';
import { toBytes } from './transport/codec.js';
import { delay } from './transport/retry.js';
import type { Transport } from './transport/transport.js';
import {
  newOperationId,
  type DeleteManyItem,
  type RangeBounds,
  type RangePage,
  type TransactionContext,
  type ValueInput,
} from './types.js';

/** The lifecycle a transaction session moves through. */
export type TransactionStatus =
  /** In progress. Its outcome is not yet decided. */
  | 'pending'
  /**
   * A commit or a rollback is in flight, so the session takes no more work. A
   * finalize that fails returns the session to `pending` so the caller can retry.
   */
  | 'finalizing'
  /** Finalized. Every change is permanent. */
  | 'committed'
  /** Reverted. Every change is discarded. */
  | 'rolledBack'
  /**
   * Definitively aborted, through a read-set conflict or a permanent two-phase
   * commit failure. This is terminal, not retryable: the server has already
   * released and removed the transaction, so the caller must start a new one.
   */
  | 'aborted';

const DEFAULT_TRANSACTION_TIMEOUT_MS = 5_000;
const MAX_LOCK_WAIT_MS = 3_000;
const MAX_LOCK_WAIT_BACKOFF_MS = 50;

export interface SessionCallOptions {
  readonly durability?: Durability;
  readonly signal?: AbortSignal;
}

export interface SessionSetOptions extends SessionCallOptions {
  readonly expiry?: number;
  readonly mode?: SetMode;
}

export interface SessionRangeOptions extends SessionCallOptions {
  readonly limit?: number;
  readonly lockMode?: RangeLockMode;
}

/**
 * One open transaction on the node that started it.
 *
 * Every operation runs against that node, because the transaction's identity lives
 * there. Dispose the session, or call {@link TransactionSession.rollback}, to
 * release what it holds; `await using` does the same through `Symbol.asyncDispose`.
 */
export class TransactionSession implements AsyncDisposable {
  #status: TransactionStatus = 'pending';

  #recordAnchorKey: string | null = null;

  #disposed = false;

  #finalizing: Promise<unknown> | null = null;

  readonly #acquiredLocks = new Set<string>();

  readonly #acquiredPrefixLocks = new Set<string>();

  readonly #timeout: number;

  constructor(
    private readonly transport: Transport,
    private readonly entryOwner: EntryOwner,
    readonly url: string,
    readonly coordinatorKey: string,
    readonly transactionId: HlcTimestamp,
    private readonly locking: TransactionLocking,
    timeoutMs: number,
    private readonly readTimestamp: HlcTimestamp = HLC_ZERO,
  ) {
    this.#timeout = timeoutMs > 0 ? timeoutMs : DEFAULT_TRANSACTION_TIMEOUT_MS;
  }

  get status(): TransactionStatus {
    return this.#status;
  }

  /** The coordinator's canonical record anchor, once the server has minted one. */
  get recordAnchorKey(): string | null {
    return this.#recordAnchorKey;
  }

  // ── Writes ─────────────────────────────────────────────────────────────────

  async set(key: string, value: ValueInput, options?: SessionSetOptions): Promise<KeyValueEntry> {
    this.requirePending();
    const durability = options?.durability ?? 'persistent';
    await this.acquireExclusiveLock(key, durability, options?.signal);

    const bytes = toBytes(value);
    const outcome = await this.transport.set(
      this.url,
      key,
      bytes,
      options?.expiry ?? 0,
      SET_MODE_FLAGS[options?.mode ?? 'always'],
      durability,
      { transaction: this.context(), signal: options?.signal },
    );

    return new KeyValueEntry(this.entryOwner, {
      key,
      success: outcome.success,
      value: bytes,
      revision: outcome.revision,
      durability,
      timeElapsedMs: outcome.timeElapsedMs,
    });
  }

  async compareValueAndSet(
    key: string,
    value: ValueInput,
    compareValue: ValueInput,
    options?: SessionSetOptions,
  ): Promise<KeyValueEntry> {
    this.requirePending();
    const durability = options?.durability ?? 'persistent';
    await this.acquireExclusiveLock(key, durability, options?.signal);

    const bytes = toBytes(value);
    const outcome = await this.transport.compareValueAndSet(
      this.url,
      key,
      bytes,
      toBytes(compareValue),
      options?.expiry ?? 0,
      durability,
      { transaction: this.context(), signal: options?.signal },
    );

    return new KeyValueEntry(this.entryOwner, {
      key,
      success: outcome.success,
      value: bytes,
      revision: outcome.revision,
      durability,
      timeElapsedMs: outcome.timeElapsedMs,
    });
  }

  async compareRevisionAndSet(
    key: string,
    value: ValueInput,
    compareRevision: number,
    options?: SessionSetOptions,
  ): Promise<KeyValueEntry> {
    this.requirePending();
    const durability = options?.durability ?? 'persistent';
    await this.acquireExclusiveLock(key, durability, options?.signal);

    const bytes = toBytes(value);
    const outcome = await this.transport.compareRevisionAndSet(
      this.url,
      key,
      bytes,
      compareRevision,
      options?.expiry ?? 0,
      durability,
      { transaction: this.context(), signal: options?.signal },
    );

    return new KeyValueEntry(this.entryOwner, {
      key,
      success: outcome.success,
      value: bytes,
      revision: outcome.revision,
      durability,
      timeElapsedMs: outcome.timeElapsedMs,
    });
  }

  async extend(
    key: string,
    expiryMs: number,
    options?: SessionCallOptions,
  ): Promise<KeyValueEntry> {
    this.requirePending();
    const durability = options?.durability ?? 'persistent';
    await this.acquireExclusiveLock(key, durability, options?.signal);

    const outcome = await this.transport.extend(this.url, key, expiryMs, durability, {
      transaction: this.context(),
      signal: options?.signal,
    });

    return new KeyValueEntry(this.entryOwner, {
      key,
      success: outcome.success,
      revision: outcome.revision,
      durability,
      timeElapsedMs: outcome.timeElapsedMs,
    });
  }

  async delete(key: string, options?: SessionCallOptions): Promise<KeyValueEntry> {
    this.requirePending();
    const durability = options?.durability ?? 'persistent';
    await this.acquireExclusiveLock(key, durability, options?.signal);

    const outcome = await this.transport.delete(this.url, key, durability, {
      transaction: this.context(),
      signal: options?.signal,
    });

    return new KeyValueEntry(this.entryOwner, {
      key,
      success: outcome.success,
      revision: outcome.revision,
      durability,
      timeElapsedMs: outcome.timeElapsedMs,
    });
  }

  async deleteMany(
    keys: readonly string[] | readonly DeleteManyItem[],
    options?: SessionCallOptions,
  ): Promise<KeyValueEntry[]> {
    this.requirePending();

    const defaultDurability = options?.durability ?? 'persistent';
    const items: DeleteManyItem[] = keys.map((entry) =>
      typeof entry === 'string'
        ? { key: entry, durability: defaultDurability }
        : { key: entry.key, durability: entry.durability ?? defaultDurability },
    );

    for (const item of items) {
      if (!item.key) continue;
      await this.acquireExclusiveLock(item.key, item.durability ?? 'persistent', options?.signal);
    }

    const outcome = await this.transport.deleteMany(this.url, items, {
      transaction: this.context(),
      signal: options?.signal,
    });

    return outcome.items.map(
      (item) =>
        new KeyValueEntry(this.entryOwner, {
          key: item.key,
          success: item.type === 'deleted',
          revision: item.revision,
          durability: item.durability,
          timeElapsedMs: outcome.timeElapsedMs,
        }),
    );
  }

  // ── Reads ──────────────────────────────────────────────────────────────────

  async get(key: string, options?: SessionCallOptions): Promise<KeyValueEntry> {
    this.requirePending();
    const durability = options?.durability ?? 'persistent';
    if (this.locking === 'pessimistic') {
      await this.acquireExclusiveLock(key, durability, options?.signal);
    }

    const outcome = await this.transport.get(this.url, key, -1, durability, {
      transaction: this.context(),
      readTimestamp: this.readTimestamp,
      signal: options?.signal,
    });

    return new KeyValueEntry(this.entryOwner, {
      key,
      success: outcome.success,
      value: outcome.value,
      revision: outcome.revision,
      durability,
      timeElapsedMs: outcome.timeElapsedMs,
      lastModified: outcome.lastModified.physical,
    });
  }

  async exists(key: string, options?: SessionCallOptions): Promise<KeyValueEntry> {
    this.requirePending();
    const durability = options?.durability ?? 'persistent';
    if (this.locking === 'pessimistic') {
      await this.acquireExclusiveLock(key, durability, options?.signal);
    }

    const outcome = await this.transport.exists(this.url, key, -1, durability, {
      transaction: this.context(),
      readTimestamp: this.readTimestamp,
      signal: options?.signal,
    });

    return new KeyValueEntry(this.entryOwner, {
      key,
      success: outcome.success,
      revision: outcome.revision,
      durability,
      timeElapsedMs: outcome.timeElapsedMs,
    });
  }

  async getByBucket(prefixKey: string, options?: SessionCallOptions): Promise<KeyValueEntry[]> {
    this.requirePending();
    const durability = options?.durability ?? 'persistent';
    if (this.locking === 'pessimistic') {
      await this.acquireExclusivePrefixLock(prefixKey, durability, options?.signal);
    }

    const items = await this.transport.getByBucket(this.url, prefixKey, durability, {
      transaction: this.context(),
      readTimestamp: this.readTimestamp,
      signal: options?.signal,
    });

    return items.map(
      (item) =>
        new KeyValueEntry(this.entryOwner, {
          key: item.key,
          success: true,
          value: item.value,
          revision: item.revision,
          durability,
          timeElapsedMs: 0,
          lastModified: item.lastModified.physical,
        }),
    );
  }

  async getByRange(bounds: RangeBounds, options?: SessionRangeOptions): Promise<RangePage> {
    this.requirePending();
    const durability = options?.durability ?? 'persistent';

    if (this.locking === 'pessimistic') {
      await this.acquireRangeLock(
        bounds,
        durability,
        options?.lockMode ?? 'exclusive',
        options?.signal,
      );
    }

    const limit = options?.limit && options.limit > 0 ? options.limit : Number.MAX_SAFE_INTEGER;
    return this.transport.getByRange(this.url, bounds, limit, durability, {
      transaction: this.context(),
      readTimestamp: this.readTimestamp,
      signal: options?.signal,
    });
  }

  // ── Finalization ───────────────────────────────────────────────────────────

  /** Commits the transaction. Returns false when the server declined to commit it. */
  async commit(options?: { signal?: AbortSignal }): Promise<boolean> {
    return this.finalize(async () => {
      try {
        const outcome = await this.transport.commitTransaction(
          this.url,
          this.coordinatorKey,
          this.transactionId,
          this.#recordAnchorKey,
          { signal: options?.signal },
        );

        if (outcome.recordAnchorKey !== null) this.#recordAnchorKey = outcome.recordAnchorKey;
        this.#status = outcome.committed ? 'committed' : 'pending';
        return outcome.committed;
      } catch (error) {
        // An abort is terminal: the server has already finalized the transaction, so
        // disposal must not roll it back again and the caller must start a new one.
        this.#status = isKeyValueCode(error, 'aborted') ? 'aborted' : 'pending';
        throw error;
      }
    }, 'commit');
  }

  /** Rolls the transaction back. Returns false when the server declined to do so. */
  async rollback(options?: { signal?: AbortSignal }): Promise<boolean> {
    return this.finalize(async () => {
      try {
        const rolledBack = await this.transport.rollbackTransaction(
          this.url,
          this.coordinatorKey,
          this.transactionId,
          this.#recordAnchorKey,
          { signal: options?.signal },
        );

        this.#status = rolledBack ? 'rolledBack' : 'pending';
        return rolledBack;
      } catch (error) {
        // An abort already released everything this rollback would have released.
        this.#status = isKeyValueCode(error, 'aborted') ? 'rolledBack' : 'pending';
        throw error;
      }
    }, 'rollback');
  }

  async [Symbol.asyncDispose](): Promise<void> {
    if (this.#disposed) return;
    this.#disposed = true;

    if (this.#status !== 'pending') return;

    // Disposal releases what the session holds. A rollback that itself fails must
    // not turn leaving a block into an exception.
    try {
      await this.rollback();
    } catch {
      // The server reclaims an abandoned session when its timeout expires.
    }
  }

  // ── Internals ──────────────────────────────────────────────────────────────

  private context(): TransactionContext {
    return {
      transactionId: this.transactionId,
      coordinatorKey: this.coordinatorKey,
      operationId: newOperationId(),
    };
  }

  private requirePending(): void {
    if (this.#status !== 'pending') {
      throw KahunaError.keyValue('Cannot perform actions on a completed transaction.', 'errored');
    }
  }

  /**
   * Runs one finalize at a time.
   *
   * Two concurrent commits would each believe they started from `pending`, so the
   * status gate is held across the whole call rather than only checked before it.
   */
  private async finalize(run: () => Promise<boolean>, verb: string): Promise<boolean> {
    if (this.#status !== 'pending') {
      throw KahunaError.keyValue(`Cannot ${verb} a transaction that is not pending.`, 'errored');
    }

    while (this.#finalizing !== null) {
      await this.#finalizing.catch(() => undefined);
      if (this.#status !== 'pending') {
        throw KahunaError.keyValue(`Cannot ${verb} a transaction that is not pending.`, 'errored');
      }
    }

    this.#status = 'finalizing';
    const attempt = run();
    this.#finalizing = attempt;

    try {
      return await attempt;
    } finally {
      this.#finalizing = null;
    }
  }

  /**
   * Takes the per-key exclusive lock, waiting while another transaction holds it.
   *
   * The wait is bounded by the shorter of the session timeout and a fixed ceiling,
   * so a contended key fails the operation instead of holding the caller forever.
   */
  private async acquireExclusiveLock(
    key: string,
    durability: Durability,
    signal: AbortSignal | undefined,
  ): Promise<void> {
    const cacheKey = `${key} ${durability}`;
    if (this.#acquiredLocks.has(cacheKey)) return;

    const deadline = Date.now() + Math.min(this.#timeout, MAX_LOCK_WAIT_MS);
    let backoffMs = 1;

    for (;;) {
      let acquired: boolean;
      let denial = 'alreadyLocked';

      try {
        acquired = await this.transport.acquireExclusiveLock(
          this.url,
          key,
          this.#timeout,
          durability,
          { transaction: this.context(), signal },
        );
      } catch (error) {
        if (isKeyValueCode(error, 'alreadyLocked') || isKeyValueCode(error, 'mustRetry')) {
          acquired = false;
          denial = (error as KahunaError).code;
        } else {
          throw error;
        }
      }

      if (acquired) {
        this.#acquiredLocks.add(cacheKey);
        return;
      }

      if (Date.now() >= deadline) {
        throw KahunaError.keyValue(
          `Failed to acquire exclusive key/value lock for '${key}': ${denial}.`,
          'mustRetry',
        );
      }

      await delay(backoffMs, signal);
      backoffMs = Math.min(backoffMs * 2, MAX_LOCK_WAIT_BACKOFF_MS);
    }
  }

  private async acquireExclusivePrefixLock(
    prefixKey: string,
    durability: Durability,
    signal: AbortSignal | undefined,
  ): Promise<void> {
    const cacheKey = `${prefixKey} ${durability}`;
    if (this.#acquiredPrefixLocks.has(cacheKey)) return;

    const acquired = await this.transport.acquireExclusivePrefixLock(
      this.url,
      prefixKey,
      this.#timeout,
      durability,
      { transaction: this.context(), signal },
    );

    if (!acquired) {
      throw KahunaError.keyValue(
        `Failed to acquire exclusive prefix lock for '${prefixKey}'.`,
        'aborted',
      );
    }

    this.#acquiredPrefixLocks.add(cacheKey);
  }

  private async acquireRangeLock(
    bounds: RangeBounds,
    durability: Durability,
    mode: RangeLockMode,
    signal: AbortSignal | undefined,
  ): Promise<void> {
    const acquired = await this.transport.acquireRangeLock(
      this.url,
      { ...bounds, mode },
      this.#timeout,
      durability,
      mode,
      { transaction: this.context(), signal },
    );

    if (!acquired) {
      throw KahunaError.keyValue(`Failed to acquire range lock for '${bounds.prefix}'.`, 'aborted');
    }
  }
}
