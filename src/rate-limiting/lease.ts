/**
 * The answer of a Kahuna-backed limiter.
 *
 * A refused lease carries {@link RateLimitLease.retryAfterMs} when the cluster
 * could tell when the budget frees up. That is what an HTTP 429 answer puts in its
 * `Retry-After` header.
 *
 * A granted lease of a concurrency limiter gives its permits back when it is
 * released, or when a `using` block disposes it. Every other lease has nothing to
 * give back: a window or a bucket refills with time, not with a release.
 */
export class RateLimitLease implements Disposable {
  /** A granted lease with nothing to release, shared because it holds no state. */
  static readonly GRANTED = new RateLimitLease(true, null, null);

  /** A refused lease that cannot say when to retry, shared because it holds no state. */
  static readonly REFUSED = new RateLimitLease(false, null, null);

  #release: (() => void) | null;

  constructor(
    /** True when the limiter granted the permits. */
    readonly acquired: boolean,
    /** On a refusal, the milliseconds until the budget can cover the request, or null when unknown. */
    readonly retryAfterMs: number | null,
    release: (() => void) | null,
  ) {
    this.#release = release;
  }

  /**
   * Gives the permits back. It runs once, however many times it is called. It
   * does not wait for the cluster: the permits come back in the background.
   */
  release(): void {
    const release = this.#release;
    this.#release = null;
    release?.();
  }

  [Symbol.dispose](): void {
    this.release();
  }
}
