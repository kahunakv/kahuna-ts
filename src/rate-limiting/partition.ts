import type { RateLimitLease } from './lease.js';
import type { AcquireOptions, KahunaRateLimiter, RateLimiterStatistics } from './limiter.js';

/** The prefix that {@link rateLimitKeyFor} puts in front of every key it names. */
export const DEFAULT_RATE_LIMIT_KEY_PREFIX = 'rate-limit/';

/**
 * Names the Kahuna key of one partition of a policy: `rate-limit/<policy>`, or
 * `rate-limit/<policy>/<partition>` when there is a partition. These are the keys
 * the .NET client names, so .NET and TypeScript replicas of one application share
 * one budget.
 *
 * The partition key is part of the stored key. Keep it free of personal data that
 * you do not want in the cluster, or hash it first.
 */
export function rateLimitKeyFor(
  policyName: string,
  partitionKey?: string | null,
  prefix: string = DEFAULT_RATE_LIMIT_KEY_PREFIX,
): string {
  if (typeof policyName !== 'string' || policyName.length === 0) {
    throw new TypeError('A rate limit policy needs a non-empty name.');
  }

  return partitionKey ? `${prefix}${policyName}/${partitionKey}` : `${prefix}${policyName}`;
}

/** Options of {@link KahunaPartitionedRateLimiter}. */
export interface PartitionedRateLimiterOptions {
  /**
   * How long a partition's limiter may stay idle before it is closed, in
   * milliseconds. A later request for that partition builds a new limiter over the
   * same key, so no budget is lost. Defaults to 10 seconds.
   */
  readonly idleTimeoutMs?: number;
}

/**
 * One Kahuna-backed limiter for each partition key, built on first use.
 *
 * Give every partition a different key, for example with {@link rateLimitKeyFor}.
 * Partitions that name the same key share one budget.
 *
 * ```ts
 * const logins = new KahunaPartitionedRateLimiter((email) =>
 *   new KahunaTokenBucketRateLimiter(client, {
 *     key: rateLimitKeyFor('login_attempts', email),
 *     tokenLimit: 5,
 *     replenishmentPeriodMs: 5 * 60_000,
 *     tokensPerPeriod: 1,
 *   }),
 * );
 *
 * const lease = await logins.acquire('alice@example.com');
 * ```
 */
export class KahunaPartitionedRateLimiter implements AsyncDisposable {
  readonly #create: (partitionKey: string) => KahunaRateLimiter;

  readonly #idleTimeoutMs: number;

  readonly #limiters = new Map<string, KahunaRateLimiter>();

  #sweepTimer: ReturnType<typeof setInterval> | null = null;

  #closed = false;

  constructor(create: (partitionKey: string) => KahunaRateLimiter, options?: PartitionedRateLimiterOptions) {
    if (typeof create !== 'function') {
      throw new TypeError('A partitioned rate limiter needs a factory function.');
    }

    const idleTimeoutMs = options?.idleTimeoutMs ?? 10_000;
    if (!Number.isFinite(idleTimeoutMs) || idleTimeoutMs < 1) {
      throw new RangeError('idleTimeoutMs must be at least one millisecond.');
    }

    this.#create = create;
    this.#idleTimeoutMs = idleTimeoutMs;
  }

  /** How many partitions have a live limiter. */
  get size(): number {
    return this.#limiters.size;
  }

  /** Asks the limiter of one partition for permits. */
  acquire(partitionKey: string, permits = 1, options?: AcquireOptions): Promise<RateLimitLease> {
    return this.limiterFor(partitionKey).acquire(permits, options);
  }

  /** The counts of one partition, or null when the partition has no live limiter. */
  getStatistics(partitionKey: string): RateLimiterStatistics | null {
    return this.#limiters.get(partitionKey)?.getStatistics() ?? null;
  }

  /** The limiter of one partition. It is built when the partition has none. */
  limiterFor(partitionKey: string): KahunaRateLimiter {
    if (this.#closed) throw new Error('The partitioned rate limiter is closed.');

    let limiter = this.#limiters.get(partitionKey);

    if (limiter === undefined || limiter.closed) {
      limiter = this.#create(partitionKey);
      this.#limiters.set(partitionKey, limiter);
      this.#startSweep();
    }

    return limiter;
  }

  /** Closes every partition's limiter. */
  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#stopSweep();

    const limiters = [...this.#limiters.values()];
    this.#limiters.clear();

    await Promise.all(limiters.map((limiter) => limiter.close()));
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }

  #startSweep(): void {
    if (this.#sweepTimer !== null) return;

    this.#sweepTimer = setInterval(() => this.#sweep(), Math.min(1_000, this.#idleTimeoutMs));
    this.#sweepTimer.unref();
  }

  #stopSweep(): void {
    if (this.#sweepTimer === null) return;

    clearInterval(this.#sweepTimer);
    this.#sweepTimer = null;
  }

  #sweep(): void {
    for (const [partitionKey, limiter] of this.#limiters) {
      const idle = limiter.idleDurationMs;

      if (idle !== null && idle >= this.#idleTimeoutMs) {
        this.#limiters.delete(partitionKey);
        void limiter.close();
      }
    }

    if (this.#limiters.size === 0) this.#stopSweep();
  }
}
