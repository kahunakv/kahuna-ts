import { OperationAbortedError, throwIfAborted } from '../errors.js';

/**
 * How many times a point operation re-issues a request the server answered with
 * `mustRetry` before it gives up.
 */
export const MUST_RETRY_ATTEMPTS = 5;

/** Base delays, in milliseconds, between `mustRetry` attempts. */
const MUST_RETRY_DELAYS_MS = [1, 2, 3, 4, 6, 8, 10];

/**
 * How long a lock release, extension or read keeps re-issuing a `mustRetry`.
 *
 * The bound is a deadline rather than a count of attempts. A leader flip or a
 * storage stall clears on a wall-clock scale, and a fixed count of attempts would
 * spend its whole budget inside the first few milliseconds.
 */
export const LOCK_MUST_RETRY_DEADLINE_MS = 30_000;

/** Returns the monotonic instant at which a lock `mustRetry` loop gives up. */
export function lockRetryDeadline(): number {
  return monotonicNow() + LOCK_MUST_RETRY_DEADLINE_MS;
}

export function monotonicNow(): number {
  return Number(process.hrtime.bigint() / 1_000_000n);
}

/** Waits the jittered delay that belongs to this attempt. */
export function waitBeforeMustRetry(attempt: number, signal?: AbortSignal): Promise<void> {
  const index = Math.min(attempt, MUST_RETRY_DELAYS_MS.length - 1);
  const base = MUST_RETRY_DELAYS_MS[index] ?? 10;
  const jittered = base * (0.75 + Math.random() * 0.5);
  return delay(jittered, signal);
}

/**
 * Decorrelated jitter back-off, the same shape the .NET client uses.
 *
 * The delay grows from about the median toward a cap and then holds, so a stuck
 * server is never busy-polled.
 */
export function* decorrelatedJitterBackoff(
  medianFirstRetryDelayMs: number,
  retryCount: number,
): Generator<number> {
  // Constants of the "decorrelated jitter v2" formula.
  const pFactor = 4.0;
  const rpScalingFactor = 1 / 1.4;
  const maxDelayMs = Number.MAX_SAFE_INTEGER;

  let previous = 0;
  let formulaIntrinsicValue = 0;
  for (let i = 0; i < retryCount; i++) {
    const t = i + Math.random();
    const next = Math.pow(2, t) * Math.tanh(Math.sqrt(pFactor * t));
    formulaIntrinsicValue = next - previous;
    previous = next;
    yield Math.min(formulaIntrinsicValue * rpScalingFactor * medianFirstRetryDelayMs, maxDelayMs);
  }
}

/**
 * Sleeps for the given number of milliseconds.
 *
 * Rejects with {@link OperationAbortedError} as soon as the signal aborts, so a
 * cancelled caller never waits out the remaining delay.
 */
export function delay(ms: number, signal?: AbortSignal): Promise<void> {
  throwIfAborted(signal);
  if (ms <= 0) return Promise.resolve();

  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);

    function onAbort(): void {
      clearTimeout(timer);
      reject(new OperationAbortedError('Operation aborted', { cause: signal?.reason }));
    }

    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

/**
 * A back-off that is built only once the first refusal arrives.
 *
 * An operation that succeeds outright is the common case, and it needs no
 * back-off state at all.
 */
export class LazyBackoff {
  private sequence: Generator<number> | null = null;

  private current = 1;

  constructor(
    private readonly medianFirstRetryDelayMs = 1,
    private readonly retryCount = 10,
  ) {}

  wait(signal?: AbortSignal): Promise<void> {
    this.sequence ??= decorrelatedJitterBackoff(this.medianFirstRetryDelayMs, this.retryCount);
    const next = this.sequence.next();
    // Past the end of the sequence the last delay is reused for every further attempt.
    if (!next.done) this.current = next.value;
    return delay(this.current, signal);
  }
}
