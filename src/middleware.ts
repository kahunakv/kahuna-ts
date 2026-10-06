/**
 * HTTP rate limiting whose budgets live in Kahuna, so every replica of an
 * application spends one budget instead of one budget each. This is the
 * TypeScript counterpart of the .NET `Kahuna.Client.AspNetCore` package.
 *
 * The module depends on no web framework. It offers:
 *
 * - {@link KahunaRateLimitPolicy}: a named policy that maps a request to a
 *   partition and asks that partition's limiter for a permit. Use it directly from
 *   any framework hook.
 * - {@link kahunaRateLimit}: a Connect-style middleware for Express, Connect and
 *   `node:http`.
 * - {@link withKahunaRateLimit}: a wrapper for Fetch-style handlers, such as those
 *   of Hono, Bun, Deno and route handlers that take a `Request`.
 *
 * ```ts
 * import { kahunaRateLimit } from 'kahuna-client/middleware';
 *
 * app.post('/login', kahunaRateLimit({
 *   client,
 *   policy: 'login_attempts',
 *   partitionBy: (req) => String(req.query.email ?? 'unknown'),
 *   tokenBucket: { tokenLimit: 5, replenishmentPeriodMs: 5 * 60_000, tokensPerPeriod: 1 },
 * }), login);
 * ```
 *
 * @module
 */

import type { IncomingMessage, ServerResponse } from 'node:http';

import type { KahunaClient } from './client.js';
import { KahunaConcurrencyLimiter } from './rate-limiting/concurrency.js';
import { KahunaFixedWindowRateLimiter } from './rate-limiting/fixed-window.js';
import type { RateLimitLease } from './rate-limiting/lease.js';
import type { AcquireOptions, KahunaRateLimiter } from './rate-limiting/limiter.js';
import {
  validateConcurrency,
  validateFixedWindow,
  validateSlidingWindow,
  validateTokenBucket,
  type ConcurrencyLimiterOptions,
  type FixedWindowRateLimiterOptions,
  type RateLimiterOptions,
  type SlidingWindowRateLimiterOptions,
  type TokenBucketRateLimiterOptions,
} from './rate-limiting/options.js';
import {
  DEFAULT_RATE_LIMIT_KEY_PREFIX,
  KahunaPartitionedRateLimiter,
  rateLimitKeyFor,
} from './rate-limiting/partition.js';
import { KahunaSlidingWindowRateLimiter } from './rate-limiting/sliding-window.js';
import { KahunaTokenBucketRateLimiter } from './rate-limiting/token-bucket.js';

/**
 * The settings of one limiter, with the key made optional. When the key is
 * absent, the policy names it: `rate-limit/<policy>`, or
 * `rate-limit/<policy>/<partition>` for a partitioned policy.
 */
export type PolicyLimiterSettings<TOptions extends RateLimiterOptions> = Omit<TOptions, 'key'> & {
  readonly key?: string;
};

/**
 * Limiter settings, or a function that builds them for one partition. The
 * function receives the partition key (null for a policy without `partitionBy`)
 * and the key the policy names for it. It runs once when the policy is built, to
 * check the settings at startup, and once for each partition the policy creates.
 */
export type PolicyLimiterConfiguration<TOptions extends RateLimiterOptions> =
  | PolicyLimiterSettings<TOptions>
  | ((partition: string | null, defaultKey: string) => PolicyLimiterSettings<TOptions>);

/** Exactly one limiter algorithm of a policy. */
export type PolicyAlgorithm =
  | {
      readonly fixedWindow: PolicyLimiterConfiguration<FixedWindowRateLimiterOptions>;
      readonly slidingWindow?: never;
      readonly tokenBucket?: never;
      readonly concurrency?: never;
    }
  | {
      readonly fixedWindow?: never;
      readonly slidingWindow: PolicyLimiterConfiguration<SlidingWindowRateLimiterOptions>;
      readonly tokenBucket?: never;
      readonly concurrency?: never;
    }
  | {
      readonly fixedWindow?: never;
      readonly slidingWindow?: never;
      readonly tokenBucket: PolicyLimiterConfiguration<TokenBucketRateLimiterOptions>;
      readonly concurrency?: never;
    }
  | {
      readonly fixedWindow?: never;
      readonly slidingWindow?: never;
      readonly tokenBucket?: never;
      readonly concurrency: PolicyLimiterConfiguration<ConcurrencyLimiterOptions>;
    };

/** The options of a {@link KahunaRateLimitPolicy}. */
export type KahunaRateLimitPolicyOptions<TRequest> = PolicyAlgorithm & {
  /** The client the limiters run their scripts through. */
  readonly client: KahunaClient;

  /** The policy name. It is part of every key the policy names. */
  readonly policy: string;

  /**
   * Returns the partition key of a request, for example a user id, an email or a
   * client address. Each partition gets a budget of its own. Without it, the
   * policy has one budget for every request.
   */
  readonly partitionBy?: (request: TRequest) => string;

  /** The prefix of every key the policy names. Defaults to `'rate-limit/'`. */
  readonly keyPrefix?: string;

  /**
   * How long a partition's limiter may stay idle in this process before it is
   * closed, in milliseconds. Defaults to 10 seconds.
   */
  readonly idleTimeoutMs?: number;
};

/**
 * A named rate-limiting policy whose budgets live in Kahuna.
 *
 * It maps each request to a partition with `partitionBy`, and asks that
 * partition's limiter for one permit. The limiter of a partition is built on first
 * use and closed after it stays idle.
 */
export class KahunaRateLimitPolicy<TRequest = unknown> implements AsyncDisposable {
  /** The policy name. */
  readonly name: string;

  readonly #partitionBy: ((request: TRequest) => string) | null;

  readonly #limiters: KahunaPartitionedRateLimiter;

  constructor(options: KahunaRateLimitPolicyOptions<TRequest>) {
    if (options === null || typeof options !== 'object') {
      throw new TypeError('A rate limit policy needs an options object.');
    }

    if (options.client === null || typeof options.client !== 'object') {
      throw new TypeError('A rate limit policy needs a KahunaClient.');
    }

    const prefix = options.keyPrefix ?? DEFAULT_RATE_LIMIT_KEY_PREFIX;
    const partitionBy = options.partitionBy ?? null;
    const build = limiterBuilder(options, prefix);

    // A setting that is wrong fails here, at startup, rather than on the first request.
    build.validate(partitionBy === null ? null : 'partition');

    this.name = options.policy;
    this.#partitionBy = partitionBy;
    this.#limiters = new KahunaPartitionedRateLimiter(
      (partition) => build.create(partitionBy === null ? null : partition),
      { idleTimeoutMs: options.idleTimeoutMs },
    );
  }

  /** The partition key of a request. It is empty for a policy without `partitionBy`. */
  partitionOf(request: TRequest): string {
    return this.#partitionBy === null ? '' : this.#partitionBy(request);
  }

  /** Asks the request's partition for permits. */
  acquire(request: TRequest, options?: AcquireOptions & { readonly permits?: number }): Promise<RateLimitLease> {
    return this.#limiters.acquire(this.partitionOf(request), options?.permits ?? 1, options);
  }

  /** The limiter of one partition key. */
  limiterFor(partitionKey: string): KahunaRateLimiter {
    return this.#limiters.limiterFor(partitionKey);
  }

  /** Closes every partition's limiter. */
  close(): Promise<void> {
    return this.#limiters.close();
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.close();
  }
}

// ── Connect-style middleware ─────────────────────────────────────────────────

/** What {@link kahunaRateLimit} and {@link withKahunaRateLimit} answer to a refusal. */
export interface RejectionOptions {
  /** The status code of a refused request. Defaults to 429. */
  readonly rejectionStatusCode?: number;
}

/** Options of {@link kahunaRateLimit}. */
export interface NodeRateLimitOptions<TRequest, TResponse> extends RejectionOptions {
  /**
   * Runs after a refusal sets the status code and the `Retry-After` header, and
   * before the response ends. It may write a body. The response is ended after it
   * when it did not end the response itself.
   */
  readonly onRejected?: (context: {
    readonly request: TRequest;
    readonly response: TResponse;
    readonly lease: RateLimitLease;
  }) => void | Promise<void>;
}

/** A Connect-style middleware, as Express, Connect and `node:http` call it. */
export type ConnectMiddleware<TRequest, TResponse> = ((
  request: TRequest,
  response: TResponse,
  next: (error?: unknown) => void,
) => void) & {
  /** The policy behind the middleware. Close it when the application stops. */
  readonly policy: KahunaRateLimitPolicy<TRequest>;
};

/**
 * Builds a Connect-style middleware that admits a request only when its policy
 * grants a permit.
 *
 * - A granted request goes on to `next()`. A concurrency permit comes back when
 *   the response closes.
 * - A refused request gets status 429 and, when the cluster could tell, a
 *   `Retry-After` header in whole seconds.
 * - A failure the policy's failure mode does not absorb goes to `next(error)`.
 * - A client that disconnects while its request waits in the queue leaves the
 *   queue, and its request gets no answer.
 */
export function kahunaRateLimit<
  TRequest extends IncomingMessage = IncomingMessage,
  TResponse extends ServerResponse = ServerResponse,
>(
  policy: KahunaRateLimitPolicy<TRequest> | KahunaRateLimitPolicyOptions<TRequest>,
  options?: NodeRateLimitOptions<TRequest, TResponse>,
): ConnectMiddleware<TRequest, TResponse> {
  const resolved = policy instanceof KahunaRateLimitPolicy ? policy : new KahunaRateLimitPolicy(policy);
  const status = options?.rejectionStatusCode ?? 429;
  const onRejected = options?.onRejected;

  const middleware = (request: TRequest, response: TResponse, next: (error?: unknown) => void): void => {
    void handle(request, response).then(
      (proceed) => {
        if (proceed) next();
      },
      (error: unknown) => {
        if (!response.destroyed) next(error);
      },
    );
  };

  /** Resolves true when the request may go on to `next()`. */
  async function handle(request: TRequest, response: TResponse): Promise<boolean> {
    const disconnected = new AbortController();
    const onClose = (): void => disconnected.abort();
    response.once('close', onClose);

    let lease: RateLimitLease;

    try {
      lease = await resolved.acquire(request, { signal: disconnected.signal });
    } catch (error) {
      // A client that left while it waited needs no answer.
      if (disconnected.signal.aborted) return false;
      throw error;
    } finally {
      response.off('close', onClose);
    }

    if (disconnected.signal.aborted) {
      lease.release();
      return false;
    }

    if (lease.acquired) {
      response.once('close', () => lease.release());
      return true;
    }

    if (response.headersSent) return false;

    response.statusCode = status;
    if (lease.retryAfterMs !== null) {
      response.setHeader('Retry-After', String(retryAfterSeconds(lease.retryAfterMs)));
    }

    await onRejected?.({ request, response, lease });

    if (!response.writableEnded) response.end();
    return false;
  }

  return Object.assign(middleware, { policy: resolved });
}

// ── Fetch-style wrapper ──────────────────────────────────────────────────────

/** Options of {@link withKahunaRateLimit}. */
export interface FetchRateLimitOptions extends RejectionOptions {
  /** Builds the answer to a refused request. Defaults to an empty 429 with `Retry-After`. */
  readonly onRejected?: (request: Request, lease: RateLimitLease) => Response | Promise<Response>;
}

/** A Fetch-style handler wrapped by {@link withKahunaRateLimit}. */
export type RateLimitedHandler<TArgs extends unknown[]> = ((
  request: Request,
  ...rest: TArgs
) => Promise<Response>) & {
  /** The policy behind the handler. Close it when the application stops. */
  readonly policy: KahunaRateLimitPolicy<Request>;
};

/**
 * Wraps a handler that takes a `Request` and returns a `Response`, so it runs only
 * when its policy grants a permit. Extra arguments, such as a route context or an
 * environment, pass through.
 *
 * The request's own signal cancels a request that waits in the queue. A
 * concurrency permit comes back when the handler's promise settles, not when a
 * streamed body ends.
 */
export function withKahunaRateLimit<TArgs extends unknown[]>(
  policy: KahunaRateLimitPolicy<Request> | KahunaRateLimitPolicyOptions<Request>,
  handler: (request: Request, ...rest: TArgs) => Response | Promise<Response>,
  options?: FetchRateLimitOptions,
): RateLimitedHandler<TArgs> {
  const resolved = policy instanceof KahunaRateLimitPolicy ? policy : new KahunaRateLimitPolicy(policy);
  const status = options?.rejectionStatusCode ?? 429;

  const wrapped = async (request: Request, ...rest: TArgs): Promise<Response> => {
    const lease = await resolved.acquire(request, { signal: request.signal });

    if (!lease.acquired) {
      if (options?.onRejected) return options.onRejected(request, lease);

      const headers = new Headers();
      if (lease.retryAfterMs !== null) {
        headers.set('Retry-After', String(retryAfterSeconds(lease.retryAfterMs)));
      }
      return new Response(null, { status, headers });
    }

    try {
      return await handler(request, ...rest);
    } finally {
      lease.release();
    }
  };

  return Object.assign(wrapped, { policy: resolved });
}

// ── Internals ────────────────────────────────────────────────────────────────

/** `Retry-After` is in whole seconds, rounded up so a client never retries too early. */
function retryAfterSeconds(ms: number): number {
  return Math.max(1, Math.ceil(ms / 1000));
}

interface LimiterBuilder {
  validate(partition: string | null): void;
  create(partition: string | null): KahunaRateLimiter;
}

function limiterBuilder<TRequest>(options: KahunaRateLimitPolicyOptions<TRequest>, prefix: string): LimiterBuilder {
  const policy = options.policy;
  const client = options.client;
  const algorithms = [options.fixedWindow, options.slidingWindow, options.tokenBucket, options.concurrency];

  if (algorithms.filter((value) => value !== undefined).length !== 1) {
    throw new TypeError(
      'A rate limit policy needs exactly one of fixedWindow, slidingWindow, tokenBucket or concurrency.',
    );
  }

  // Checks the policy name before any settings function sees a key built from it.
  rateLimitKeyFor(policy, null, prefix);

  if (options.fixedWindow !== undefined) {
    return builder(policy, prefix, options.fixedWindow, validateFixedWindow, (settings) => new KahunaFixedWindowRateLimiter(client, settings));
  }

  if (options.slidingWindow !== undefined) {
    return builder(policy, prefix, options.slidingWindow, validateSlidingWindow, (settings) => new KahunaSlidingWindowRateLimiter(client, settings));
  }

  if (options.tokenBucket !== undefined) {
    return builder(policy, prefix, options.tokenBucket, validateTokenBucket, (settings) => new KahunaTokenBucketRateLimiter(client, settings));
  }

  return builder(policy, prefix, options.concurrency!, validateConcurrency, (settings) => new KahunaConcurrencyLimiter(client, settings));
}

function builder<TOptions extends RateLimiterOptions>(
  policy: string,
  prefix: string,
  configuration: PolicyLimiterConfiguration<TOptions>,
  validate: (options: TOptions) => unknown,
  construct: (options: TOptions) => KahunaRateLimiter,
): LimiterBuilder {
  const settingsFor = (partition: string | null): TOptions => {
    const defaultKey = rateLimitKeyFor(policy, partition, prefix);
    const settings = typeof configuration === 'function' ? configuration(partition, defaultKey) : configuration;
    return { ...settings, key: settings.key ?? defaultKey } as unknown as TOptions;
  };

  return {
    validate: (partition) => void validate(settingsFor(partition)),
    create: (partition) => construct(settingsFor(partition)),
  };
}
