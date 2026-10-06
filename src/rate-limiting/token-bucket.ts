import type { KahunaClient } from '../client.js';
import { RateLimitLease } from './lease.js';
import { KahunaRateLimiter } from './limiter.js';
import { validateTokenBucket, type TokenBucketRateLimiterOptions } from './options.js';
import { TOKEN_BUCKET_SCRIPT, selectScript, type RateLimiterScript } from './scripts.js';

/**
 * A token-bucket limiter whose bucket lives in Kahuna, shared by every process
 * that names the same key. A burst may spend up to `tokenLimit` tokens, and the
 * bucket then refills by `tokensPerPeriod` tokens every `replenishmentPeriodMs`.
 *
 * No process runs a refill timer. Each decision computes the refill from the
 * cluster clock, so the bucket refills on schedule even while no process runs.
 */
export class KahunaTokenBucketRateLimiter extends KahunaRateLimiter {
  readonly #script: RateLimiterScript;

  readonly #limit: string;

  readonly #period: string;

  readonly #perPeriod: string;

  constructor(client: KahunaClient, options: TokenBucketRateLimiterOptions) {
    super(client, validateTokenBucket(options), options.tokenLimit);

    this.#script = selectScript(this.durability, TOKEN_BUCKET_SCRIPT);
    this.#limit = String(options.tokenLimit);
    this.#period = String(options.replenishmentPeriodMs);
    this.#perPeriod = String(options.tokensPerPeriod);
  }

  protected override async attemptOnCluster(
    permits: number,
    signal: AbortSignal | undefined,
  ): Promise<RateLimitLease> {
    const { granted, value } = await this.decide(
      this.#script,
      [
        { key: '@key', value: this.key },
        { key: '@limit', value: this.#limit },
        { key: '@period_ms', value: this.#period },
        { key: '@per_period', value: this.#perPeriod },
        { key: '@permits', value: String(permits) },
      ],
      signal,
    );

    if (!granted) return this.refusal(value);

    this.observeAvailablePermits(value);
    return RateLimitLease.GRANTED;
  }
}
