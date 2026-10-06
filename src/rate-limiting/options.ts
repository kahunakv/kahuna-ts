import type { Durability } from '../enums.js';

/**
 * What a Kahuna-backed limiter answers when it cannot reach a decision, because
 * the cluster is unreachable or every retry of a contended counter aborted.
 *
 * - `'throw'` surfaces the failure to the caller. Nothing is admitted or refused
 *   silently.
 * - `'allow'` admits the request. The limit stops protecting the application while
 *   the cluster is down, but the application keeps serving. The admitted lease
 *   holds no permit on the cluster.
 * - `'deny'` refuses the request, as if the limit were spent.
 */
export type RateLimiterFailureMode = 'throw' | 'allow' | 'deny';

/**
 * Which waiter of the local queue is served first. `'newestFirst'` also evicts
 * the oldest waiters, with a refused lease, to make room for a new one when the
 * queue is full.
 */
export type QueueProcessingOrder = 'oldestFirst' | 'newestFirst';

/**
 * Settings every Kahuna-backed limiter shares.
 *
 * The counter lives in Kahuna, so every process that names the same `key` with
 * the same limits spends one budget between them. Two processes that name one key
 * with different limits do not agree on anything: each one judges the shared
 * counter by its own numbers.
 */
export interface RateLimiterOptions {
  /**
   * The Kahuna key that holds this limiter's state. Every limiter that must share
   * a budget names the same key, and every limiter that must not share one names
   * a different key.
   */
  readonly key: string;

  /**
   * Where the state lives. `'ephemeral'` keeps it in memory on the cluster: it is
   * far cheaper, and a restart of the whole cluster only resets the budgets.
   * `'persistent'` replicates and persists every admission. Defaults to
   * `'ephemeral'`.
   */
  readonly durability?: Durability;

  /**
   * How many permits may wait in this process for the budget to free up. Zero,
   * the default, refuses at once. The queue is local to the process: each process
   * waits on its own queue for the shared budget.
   */
  readonly queueLimit?: number;

  /** Which waiter is served first. Defaults to `'oldestFirst'`. */
  readonly queueProcessingOrder?: QueueProcessingOrder;

  /** What the limiter answers when it cannot reach a decision. Defaults to `'throw'`. */
  readonly failureMode?: RateLimiterFailureMode;

  /**
   * How many times a decision that aborted on a contended counter is attempted
   * again before `failureMode` applies. An aborted attempt changed nothing, so a
   * retry cannot spend a permit twice. Defaults to 8.
   */
  readonly maxRetries?: number;
}

/** Settings of {@link KahunaFixedWindowRateLimiter}. */
export interface FixedWindowRateLimiterOptions extends RateLimiterOptions {
  /** How many permits one window holds. */
  readonly permitLimit: number;

  /**
   * The length of one window, in whole milliseconds. Windows are aligned to the
   * cluster clock, so every process that shares the key agrees on where a window
   * starts.
   */
  readonly windowMs: number;
}

/** The largest accepted `segmentsPerWindow` of a sliding window. */
export const MAX_SEGMENTS_PER_WINDOW = 1000;

/** Settings of {@link KahunaSlidingWindowRateLimiter}. */
export interface SlidingWindowRateLimiterOptions extends RateLimiterOptions {
  /** How many permits the window holds at any moment. */
  readonly permitLimit: number;

  /**
   * The length of the window, in milliseconds. It is cut into `segmentsPerWindow`
   * segments of whole milliseconds, so the effective window is rounded down to a
   * multiple of the segment count.
   */
  readonly windowMs: number;

  /**
   * How many segments the window is cut into, from 1 to
   * {@link MAX_SEGMENTS_PER_WINDOW}. The permits of a segment come back when that
   * segment slides out of the window. More segments track the window more closely
   * and make each decision cost more, because the whole window is one value on
   * the cluster.
   */
  readonly segmentsPerWindow: number;
}

/** Settings of {@link KahunaTokenBucketRateLimiter}. */
export interface TokenBucketRateLimiterOptions extends RateLimiterOptions {
  /** How many tokens the bucket holds when it is full. A new bucket starts full. */
  readonly tokenLimit: number;

  /**
   * How often the bucket gains `tokensPerPeriod` tokens, in whole milliseconds.
   * Replenishment is computed from the cluster clock when a request arrives, so no
   * process runs a timer for it.
   */
  readonly replenishmentPeriodMs: number;

  /** How many tokens one period adds, up to `tokenLimit`. */
  readonly tokensPerPeriod: number;
}

/** Settings of {@link KahunaConcurrencyLimiter}. */
export interface ConcurrencyLimiterOptions extends RateLimiterOptions {
  /** How many permits may be held at once, across every process that shares the key. */
  readonly permitLimit: number;

  /**
   * How long a held permit survives on the cluster without a renewal, in
   * milliseconds. The process that holds a permit renews it every third of this
   * period until the lease is released, so a permit only lapses when its holder
   * dies or loses the cluster. A shorter lease returns the permits of a dead
   * process sooner, and costs more renewals. Defaults to 30 seconds.
   */
  readonly leaseDurationMs?: number;

  /**
   * How long a queued waiter sleeps before it asks the cluster again, in
   * milliseconds. A permit released by another process is only seen at the next
   * attempt. A permit released by this process wakes the queue at once. Defaults
   * to 50.
   */
  readonly queuePollIntervalMs?: number;
}

/** The common settings with every default filled in. */
export interface ResolvedRateLimiterOptions {
  readonly key: string;
  readonly durability: Durability;
  readonly queueLimit: number;
  readonly queueProcessingOrder: QueueProcessingOrder;
  readonly failureMode: RateLimiterFailureMode;
  readonly maxRetries: number;
}

export function resolveCommon(options: RateLimiterOptions): ResolvedRateLimiterOptions {
  if (options === null || typeof options !== 'object') {
    throw new TypeError('A Kahuna rate limiter needs an options object.');
  }

  if (typeof options.key !== 'string' || options.key.length === 0) {
    throw new TypeError('A Kahuna rate limiter needs a non-empty key.');
  }

  const resolved: ResolvedRateLimiterOptions = {
    key: options.key,
    durability: options.durability ?? 'ephemeral',
    queueLimit: options.queueLimit ?? 0,
    queueProcessingOrder: options.queueProcessingOrder ?? 'oldestFirst',
    failureMode: options.failureMode ?? 'throw',
    maxRetries: options.maxRetries ?? 8,
  };

  requireInteger(resolved.queueLimit, 'queueLimit', 0);
  requireInteger(resolved.maxRetries, 'maxRetries', 0);

  return resolved;
}

export function validateFixedWindow(options: FixedWindowRateLimiterOptions): ResolvedRateLimiterOptions {
  const common = resolveCommon(options);
  requireInteger(options.permitLimit, 'permitLimit', 1);
  requireInteger(options.windowMs, 'windowMs', 1);
  return common;
}

export function validateSlidingWindow(options: SlidingWindowRateLimiterOptions): ResolvedRateLimiterOptions {
  const common = resolveCommon(options);
  requireInteger(options.permitLimit, 'permitLimit', 1);
  requireInteger(options.segmentsPerWindow, 'segmentsPerWindow', 1);

  if (options.segmentsPerWindow > MAX_SEGMENTS_PER_WINDOW) {
    throw new RangeError(`segmentsPerWindow must be between 1 and ${MAX_SEGMENTS_PER_WINDOW}.`);
  }

  requireInteger(options.windowMs, 'windowMs', 1);
  if (options.windowMs < options.segmentsPerWindow) {
    throw new RangeError('windowMs must hold at least one millisecond per segment.');
  }

  return common;
}

export function validateTokenBucket(options: TokenBucketRateLimiterOptions): ResolvedRateLimiterOptions {
  const common = resolveCommon(options);
  requireInteger(options.tokenLimit, 'tokenLimit', 1);
  requireInteger(options.tokensPerPeriod, 'tokensPerPeriod', 1);
  requireInteger(options.replenishmentPeriodMs, 'replenishmentPeriodMs', 1);
  return common;
}

export function validateConcurrency(options: ConcurrencyLimiterOptions): ResolvedRateLimiterOptions {
  const common = resolveCommon(options);
  requireInteger(options.permitLimit, 'permitLimit', 1);
  requireInteger(options.leaseDurationMs ?? 30_000, 'leaseDurationMs', 30);
  requireInteger(options.queuePollIntervalMs ?? 50, 'queuePollIntervalMs', 1);
  return common;
}

function requireInteger(value: number, name: string, minimum: number): void {
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new RangeError(`${name} must be an integer of at least ${minimum}.`);
  }
}
