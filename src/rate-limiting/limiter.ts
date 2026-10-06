import type { Durability } from '../enums.js';
import { KahunaError, OperationAbortedError, isKeyValueCode, throwIfAborted } from '../errors.js';
import type { ScriptRunner } from '../transaction-script.js';
import { bytesToText } from '../transport/codec.js';
import type { ScriptParameter, ScriptResult } from '../types.js';
import { RateLimitLease } from './lease.js';
import type {
  QueueProcessingOrder,
  RateLimiterFailureMode,
  ResolvedRateLimiterOptions,
} from './options.js';
import type { RateLimiterScript } from './scripts.js';

/** Counts that one limiter observed in this process. */
export interface RateLimiterStatistics {
  /**
   * The permits the cluster reported as left on the last decision. Other
   * processes may have spent them since.
   */
  readonly currentAvailablePermits: number;
  /** The permits that wait in this process's queue. */
  readonly currentQueuedCount: number;
  readonly totalFailedLeases: number;
  readonly totalSuccessfulLeases: number;
}

/** Options one acquisition accepts. */
export interface AcquireOptions {
  /** Cancels the acquisition. A queued request leaves the queue. */
  readonly signal?: AbortSignal;
}

/** The longest delay `setTimeout` accepts. */
const MAX_TIMER_MS = 2_147_483_647;

/**
 * Base of the limiters whose budget lives in Kahuna, so every process that names
 * the same key spends one budget between them.
 *
 * A decision needs a round trip to the cluster, so {@link KahunaRateLimiter.acquire}
 * is asynchronous and no synchronous attempt exists.
 *
 * A request that the budget refuses can wait in a queue that is local to this
 * process. One background loop serves the queue: it asks the cluster again for the
 * waiter at the head, and sleeps in between for as long as the cluster said the
 * budget stays spent.
 */
export abstract class KahunaRateLimiter implements AsyncDisposable {
  /** The Kahuna key that holds this limiter's state. */
  readonly key: string;

  /** The largest permit count one request may ask for. */
  readonly permitLimit: number;

  protected readonly durability: Durability;

  readonly #runner: ScriptRunner;

  // The settings are copied at construction, so a later change to the options object changes nothing.
  readonly #queueLimit: number;

  readonly #queueProcessingOrder: QueueProcessingOrder;

  readonly #failureMode: RateLimiterFailureMode;

  readonly #maxRetries: number;

  readonly #queue: Waiter[] = [];

  /** Aborted by {@link KahunaRateLimiter.close}. Stops the queue loop and every background loop. */
  readonly #closeController = new AbortController();

  #closing: Promise<void> | null = null;

  #queuedPermits = 0;

  #queueLoopRunning = false;

  /** Requests inside {@link KahunaRateLimiter.acquire}, queued ones included. */
  #activeRequests = 0;

  #idleSince = performance.now();

  #successfulLeases = 0;

  #failedLeases = 0;

  #lastKnownAvailablePermits: number;

  /** Bumped on every wake, so the queue loop sees a wake that came during its attempt. */
  #wakeVersion = 0;

  readonly #wakeListeners = new Set<() => void>();

  protected constructor(runner: ScriptRunner, options: ResolvedRateLimiterOptions, permitLimit: number) {
    this.#runner = runner;
    this.key = options.key;
    this.durability = options.durability;
    this.#queueLimit = options.queueLimit;
    this.#queueProcessingOrder = options.queueProcessingOrder;
    this.#failureMode = options.failureMode;
    this.#maxRetries = options.maxRetries;
    this.permitLimit = permitLimit;
    this.#lastKnownAvailablePermits = permitLimit;
  }

  /** True once {@link KahunaRateLimiter.close} was called. */
  get closed(): boolean {
    return this.#closeController.signal.aborted;
  }

  /**
   * The milliseconds this limiter has done nothing for, or null while it has work
   * in progress. A partitioned limiter closes a partition that stays idle, and a
   * later request for that partition builds a new limiter over the same key, so no
   * budget is lost.
   */
  get idleDurationMs(): number | null {
    if (this.#activeRequests > 0 || this.hasHeldPermits) return null;
    return performance.now() - this.#idleSince;
  }

  /** Counts that this process observed. */
  getStatistics(): RateLimiterStatistics {
    return {
      currentAvailablePermits: this.#lastKnownAvailablePermits,
      currentQueuedCount: this.#queuedPermits,
      totalFailedLeases: this.#failedLeases,
      totalSuccessfulLeases: this.#successfulLeases,
    };
  }

  /**
   * Asks for permits. Resolves with a granted lease or a refused one. Zero permits
   * asks whether permits are free and takes none.
   *
   * Rejects with a `RangeError` for a permit count above
   * {@link KahunaRateLimiter.permitLimit}, with {@link OperationAbortedError} when
   * the signal aborts, and with the cluster's failure when `failureMode` is
   * `'throw'`.
   */
  async acquire(permits = 1, options?: AcquireOptions): Promise<RateLimitLease> {
    this.#validatePermits(permits);
    this.#throwIfClosed();
    throwIfAborted(options?.signal);

    this.#activeRequests++;

    try {
      // A new request does not overtake older waiters, unless the queue serves the newest first.
      const attemptNow = this.#queue.length === 0 || this.#queueProcessingOrder === 'newestFirst';

      let refusal: RateLimitLease | null = null;

      if (attemptNow) {
        const lease = await this.#attempt(permits, options?.signal);

        if (lease.acquired) {
          this.#successfulLeases++;
          return lease;
        }

        refusal = lease;
      }

      if (this.#queueLimit === 0 || permits > this.#queueLimit) {
        this.#failedLeases++;
        return refusal ?? RateLimitLease.REFUSED;
      }

      return await this.#waitInQueue(permits, refusal, options?.signal);
    } finally {
      if (--this.#activeRequests === 0) this.#idleSince = performance.now();
    }
  }

  /**
   * Stops the queue and refuses every waiter. A lease already handed out stays
   * valid, and a concurrency lease still gives its permits back when it is
   * released. The returned promise settles when background releases finish.
   */
  close(): Promise<void> {
    if (this.#closing !== null) return this.#closing;

    this.#closeController.abort();

    const drained = this.#queue.splice(0);
    this.#queuedPermits = 0;

    for (const waiter of drained) {
      waiter.queued = false;
      if (waiter.succeed(RateLimitLease.REFUSED)) this.#failedLeases++;
    }

    this.wakeQueue();

    this.#closing = this.waitForBackgroundWork();
    return this.#closing;
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }

  /**
   * Asks the cluster for permits once. Returns a granted lease or a refused one.
   * Throws for cancellation, or for a failure the failure mode decides on.
   */
  protected abstract attemptOnCluster(permits: number, signal: AbortSignal | undefined): Promise<RateLimitLease>;

  /** True while this process holds permits that it must give back. */
  protected get hasHeldPermits(): boolean {
    return false;
  }

  /** How long the queue sleeps after a refusal that could not say when the budget frees up. */
  protected get queuePollIntervalMs(): number {
    return 50;
  }

  /** Aborted when the limiter closes. */
  protected get closeSignal(): AbortSignal {
    return this.#closeController.signal;
  }

  /** Waits for work a subclass runs after a lease is gone, such as giving permits back. */
  protected waitForBackgroundWork(): Promise<void> {
    return Promise.resolve();
  }

  /**
   * Runs one script, and runs it again while the cluster answers that the
   * transaction aborted or must be retried. Neither outcome changed the state, so a
   * retry cannot spend permits twice. Two callers that race for one counter are
   * what makes a transaction abort.
   */
  protected async runScript(
    script: RateLimiterScript,
    parameters: readonly ScriptParameter[],
    signal?: AbortSignal,
  ): Promise<ScriptResult> {
    for (let attempt = 0; ; attempt++) {
      try {
        return await this.#runner.executeScript(script.bytes, { hash: script.hash, parameters, signal });
      } catch (error) {
        const retryable = isKeyValueCode(error, 'aborted') || isKeyValueCode(error, 'mustRetry');
        if (!retryable || attempt >= this.#maxRetries || signal?.aborted) throw error;

        await sleep(retryDelayMs(attempt), signal);
      }
    }
  }

  /** Runs a decision script and reads its `"1:<n>"` or `"0:<ms>"` answer. */
  protected async decide(
    script: RateLimiterScript,
    parameters: readonly ScriptParameter[],
    signal: AbortSignal | undefined,
  ): Promise<{ granted: boolean; value: number }> {
    return parseAnswer(await this.runScript(script, parameters, signal));
  }

  /** Records the permits the cluster reported as left, for the statistics. */
  protected observeAvailablePermits(available: number): void {
    this.#lastKnownAvailablePermits = Math.max(0, available);
  }

  /**
   * A refusal that carries the time until the budget can cover the request, when
   * the cluster could tell. Statistics treat a refusal as zero permits left.
   */
  protected refusal(retryAfterMs: number): RateLimitLease {
    this.observeAvailablePermits(0);
    return retryAfterMs > 0 ? new RateLimitLease(false, retryAfterMs, null) : RateLimitLease.REFUSED;
  }

  /** Wakes the queue loop before its sleep ends, because permits came back. */
  protected wakeQueue(): void {
    this.#wakeVersion++;
    for (const listener of [...this.#wakeListeners]) listener();
  }

  async #attempt(permits: number, signal: AbortSignal | undefined): Promise<RateLimitLease> {
    try {
      return await this.attemptOnCluster(permits, signal);
    } catch (error) {
      // The caller asked for the cancellation, so it gets the cancellation.
      if (signal?.aborted) {
        if (error instanceof OperationAbortedError) throw error;
        throw new OperationAbortedError('Operation aborted', { cause: signal.reason });
      }

      if (this.#failureMode === 'allow') return RateLimitLease.GRANTED;
      if (this.#failureMode === 'deny') return RateLimitLease.REFUSED;
      throw error;
    }
  }

  #waitInQueue(
    permits: number,
    refusal: RateLimitLease | null,
    signal: AbortSignal | undefined,
  ): Promise<RateLimitLease> {
    this.#throwIfClosed();

    if (this.#queuedPermits + permits > this.#queueLimit) {
      if (this.#queueProcessingOrder === 'oldestFirst') {
        this.#failedLeases++;
        return Promise.resolve(refusal ?? RateLimitLease.REFUSED);
      }

      // The newest is served first, so the oldest waiters make room for it.
      while (this.#queuedPermits + permits > this.#queueLimit && this.#queue.length > 0) {
        const oldest = this.#queue[0]!;
        this.#removeFromQueue(oldest);
        if (oldest.succeed(RateLimitLease.REFUSED)) this.#failedLeases++;
      }
    }

    return new Promise<RateLimitLease>((resolve, reject) => {
      const waiter = new Waiter(permits, resolve, reject);

      if (signal) {
        const onAbort = (): void => {
          this.#removeFromQueue(waiter);
          waiter.fail(new OperationAbortedError('Operation aborted', { cause: signal.reason }));
        };

        signal.addEventListener('abort', onAbort, { once: true });
        waiter.cleanup = () => signal.removeEventListener('abort', onAbort);
      }

      this.#queue.push(waiter);
      this.#queuedPermits += permits;

      if (!this.#queueLoopRunning) {
        this.#queueLoopRunning = true;
        void this.#serveQueue();
      }
    });
  }

  #removeFromQueue(waiter: Waiter): void {
    if (!waiter.queued) return;

    const index = this.#queue.indexOf(waiter);
    if (index >= 0) this.#queue.splice(index, 1);

    waiter.queued = false;
    this.#queuedPermits -= waiter.permits;
  }

  /**
   * Serves the queue until it is empty. It asks the cluster for the waiter that is
   * next in order, and hands it the lease when granted. Otherwise it sleeps until
   * the cluster said the budget can cover it, or until this process gives permits
   * back.
   *
   * The waiter stays in the queue while its attempt runs, so it keeps counting
   * against the queue limit. A waiter that was cancelled or evicted during the
   * attempt can no longer take the lease, and a granted lease it cannot take is
   * released at once to give the permits back.
   */
  async #serveQueue(): Promise<void> {
    const closeSignal = this.#closeController.signal;

    while (true) {
      const waiter = this.#queueProcessingOrder === 'oldestFirst' ? this.#queue[0] : this.#queue.at(-1);

      if (waiter === undefined || closeSignal.aborted) {
        this.#queueLoopRunning = false;
        return;
      }

      // Read before the attempt, so permits given back while the attempt runs still skip the sleep.
      const wakeVersion = this.#wakeVersion;

      let lease: RateLimitLease;

      try {
        lease = await this.#attempt(waiter.permits, closeSignal);
      } catch (error) {
        if (closeSignal.aborted) {
          // Closing already completed every waiter.
          this.#queueLoopRunning = false;
          return;
        }

        // The failure mode is to throw, so the waiter the attempt was for gets the failure.
        this.#removeFromQueue(waiter);
        waiter.fail(error);
        continue;
      }

      if (lease.acquired) {
        this.#removeFromQueue(waiter);

        if (waiter.succeed(lease)) this.#successfulLeases++;
        else lease.release();

        continue;
      }

      // The waiter left during the attempt, so the next one is asked at once.
      if (!waiter.queued || wakeVersion !== this.#wakeVersion) continue;

      await this.#sleepUntilWoken(lease.retryAfterMs ?? this.queuePollIntervalMs, closeSignal);
    }
  }

  #sleepUntilWoken(ms: number, signal: AbortSignal): Promise<void> {
    return new Promise<void>((resolve) => {
      const done = (): void => {
        clearTimeout(timer);
        this.#wakeListeners.delete(done);
        signal.removeEventListener('abort', done);
        resolve();
      };

      const timer = setTimeout(done, Math.min(ms, MAX_TIMER_MS));
      this.#wakeListeners.add(done);
      signal.addEventListener('abort', done, { once: true });
    });
  }

  #validatePermits(permits: number): void {
    if (!Number.isSafeInteger(permits) || permits < 0 || permits > this.permitLimit) {
      throw new RangeError(`${permits} permits exceeds the permit limit of ${this.permitLimit}.`);
    }
  }

  #throwIfClosed(): void {
    if (this.closed) throw new Error(`The rate limiter for '${this.key}' is closed.`);
  }
}

class Waiter {
  queued = true;

  #settled = false;

  cleanup: (() => void) | null = null;

  constructor(
    readonly permits: number,
    private readonly resolve: (lease: RateLimitLease) => void,
    private readonly reject: (error: unknown) => void,
  ) {}

  /** Hands the waiter its lease. False when the waiter already settled. */
  succeed(lease: RateLimitLease): boolean {
    if (this.#settled) return false;
    this.#settle();
    this.resolve(lease);
    return true;
  }

  fail(error: unknown): void {
    if (this.#settled) return;
    this.#settle();
    this.reject(error);
  }

  #settle(): void {
    this.#settled = true;
    this.cleanup?.();
    this.cleanup = null;
  }
}

/**
 * Sleeps, and rejects with {@link OperationAbortedError} when the signal aborts.
 * An unreferenced sleep does not keep the process alive.
 */
export function sleep(ms: number, signal?: AbortSignal, unref = false): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    if (signal?.aborted) {
      reject(new OperationAbortedError('Operation aborted', { cause: signal.reason }));
      return;
    }

    const onAbort = (): void => {
      clearTimeout(timer);
      reject(new OperationAbortedError('Operation aborted', { cause: signal?.reason }));
    };

    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, Math.min(ms, MAX_TIMER_MS));

    if (unref) timer.unref();
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

/**
 * A random delay that grows with the attempt, so callers that collided once do not
 * collide again in lockstep: up to 2, 4, 8, … ms, and never more than 64 ms.
 */
function retryDelayMs(attempt: number): number {
  const ceiling = 2 << Math.min(attempt, 5);
  return 1 + Math.floor(Math.random() * (ceiling - 1));
}

/** Reads `"1:<n>"` or `"0:<n>"`, the only answers the decision scripts give. */
function parseAnswer(result: ScriptResult): { granted: boolean; value: number } {
  const answer = bytesToText(result.values[0]?.value ?? null) ?? '';
  const match = /^([01]):(-?\d+)$/.exec(answer);

  if (match) return { granted: match[1] === '1', value: Number(match[2]) };

  throw KahunaError.keyValue(
    `Rate limiter script returned an unexpected answer '${answer}' (${result.type})`,
    'errored',
  );
}
