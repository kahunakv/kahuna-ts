import type { KahunaClient } from '../client.js';
import { RateLimitLease } from './lease.js';
import { KahunaRateLimiter } from './limiter.js';
import { validateSlidingWindow, type SlidingWindowRateLimiterOptions } from './options.js';
import { SLIDING_WINDOW_SCRIPT, selectScript, type RateLimiterScript } from './scripts.js';

/**
 * A sliding-window limiter whose budget lives in Kahuna: at most `permitLimit`
 * permits in any window, across every process that names the same key.
 *
 * The window is cut into segments. The permits spent in a segment come back when
 * that segment slides out of the window, so a burst at the end of one window cannot
 * be followed by a full burst at the start of the next, which a fixed window
 * allows.
 */
export class KahunaSlidingWindowRateLimiter extends KahunaRateLimiter {
  readonly #script: RateLimiterScript;

  readonly #limit: string;

  readonly #segment: string;

  readonly #segments: string;

  constructor(client: KahunaClient, options: SlidingWindowRateLimiterOptions) {
    super(client, validateSlidingWindow(options), options.permitLimit);

    this.#script = selectScript(this.durability, SLIDING_WINDOW_SCRIPT);
    this.#limit = String(options.permitLimit);
    this.#segment = String(Math.floor(options.windowMs / options.segmentsPerWindow));
    this.#segments = String(options.segmentsPerWindow);
  }

  protected override async attemptOnCluster(
    permits: number,
    signal: AbortSignal | undefined,
  ): Promise<RateLimitLease> {
    const { granted, value } = await this.decide(
      this.#script,
      [
        { key: '@key', value: this.key },
        { key: '@segment_ms', value: this.#segment },
        { key: '@segments', value: this.#segments },
        { key: '@limit', value: this.#limit },
        { key: '@permits', value: String(permits) },
      ],
      signal,
    );

    if (!granted) return this.refusal(value);

    this.observeAvailablePermits(value);
    return RateLimitLease.GRANTED;
  }
}
