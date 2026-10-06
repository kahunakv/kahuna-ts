import type { KahunaClient } from '../client.js';
import { RateLimitLease } from './lease.js';
import { KahunaRateLimiter } from './limiter.js';
import { validateFixedWindow, type FixedWindowRateLimiterOptions } from './options.js';
import { FIXED_WINDOW_SCRIPT, selectScript, type RateLimiterScript } from './scripts.js';

/**
 * A fixed-window limiter whose budget lives in Kahuna: at most `permitLimit`
 * permits per window, across every process that names the same key. Windows are
 * aligned to the cluster clock, and the budget resets on each window boundary
 * whatever happened in the window before.
 */
export class KahunaFixedWindowRateLimiter extends KahunaRateLimiter {
  readonly #script: RateLimiterScript;

  readonly #limit: string;

  readonly #window: string;

  constructor(client: KahunaClient, options: FixedWindowRateLimiterOptions) {
    super(client, validateFixedWindow(options), options.permitLimit);

    this.#script = selectScript(this.durability, FIXED_WINDOW_SCRIPT);
    this.#limit = String(options.permitLimit);
    this.#window = String(options.windowMs);
  }

  protected override async attemptOnCluster(
    permits: number,
    signal: AbortSignal | undefined,
  ): Promise<RateLimitLease> {
    const { granted, value } = await this.decide(
      this.#script,
      [
        { key: '@key', value: this.key },
        { key: '@window_ms', value: this.#window },
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
