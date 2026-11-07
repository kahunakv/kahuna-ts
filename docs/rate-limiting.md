# Rate limiting

A rate limiter inside one process counts the requests of that process only. If
your application runs as 3 replicas, a limit of 100 requests per minute admits 300.

The Kahuna rate limiters keep the count in Kahuna. All replicas that use the same
key share one budget. The .NET client uses the same scripts and keys, so .NET and
TypeScript replicas share one budget too.

## Limiters

| Limiter | Budget | Typical use |
| --- | --- | --- |
| `KahunaFixedWindowRateLimiter` | `permitLimit` permits per window. The budget resets at the start of each window. | Simple endpoints. |
| `KahunaSlidingWindowRateLimiter` | `permitLimit` permits in any window. The permits of a segment come back when the segment leaves the window. | Smooth limits. |
| `KahunaTokenBucketRateLimiter` | Bursts up to `tokenLimit`. Adds `tokensPerPeriod` tokens every `replenishmentPeriodMs`. | Login attempts, checkout. |
| `KahunaConcurrencyLimiter` | `permitLimit` requests in progress at once. A permit comes back when you release its lease. | Expensive operations. |

```ts
import { KahunaTokenBucketRateLimiter } from 'kahuna-client';

const logins = new KahunaTokenBucketRateLimiter(client, {
  key: 'rate-limit/login_attempts',
  tokenLimit: 5,
  replenishmentPeriodMs: 5 * 60_000,
  tokensPerPeriod: 1,
});

const lease = await logins.acquire();
if (!lease.acquired) {
  console.log(`retry in ${lease.retryAfterMs} ms`);
}
```

Each decision is one [script transaction](transactions.md#scripts). Two replicas
that race for the last permit cannot both get it. The time comes from the cluster
clock, so all replicas agree on the windows and the token counts.

A concurrency lease holds its permits until you release it. `using` releases it at
the end of the block:

```ts
using lease = await reports.acquire();
if (lease.acquired) await generateReport();
```

## Options

| Option | Default | Meaning |
| --- | --- | --- |
| `key` | — | The Kahuna key of the budget. Required. |
| `durability` | `'ephemeral'` | `'persistent'` replicates and stores every admission. |
| `queueLimit` | `0` | How many requests can wait in this process. `0` refuses at once. |
| `queueProcessingOrder` | `'oldestFirst'` | `'newestFirst'` also drops the oldest waiter when the queue is full. |
| `failureMode` | `'throw'` | When the cluster is unreachable: `'throw'` throws, `'allow'` admits, `'deny'` refuses. |
| `maxRetries` | `8` | Retries of a decision that aborted because another replica changed the counter. |

## One budget per user

`KahunaPartitionedRateLimiter` keeps one limiter per partition key, such as a user
ID or a client IP address. `rateLimitKeyFor(policy, partition)` returns the key
`rate-limit/<policy>/<partition>`.

## HTTP middleware

`kahuna-client/middleware` does the same job as the .NET
`Kahuna.Client.AspNetCore` package. It depends on no web framework.

```ts
import express from 'express';
import { kahunaRateLimit } from 'kahuna-client/middleware';

const app = express();

// 100 requests per minute, across all replicas.
app.use('/api', kahunaRateLimit({
  client,
  policy: 'simple_endpoints',
  fixedWindow: { permitLimit: 100, windowMs: 60_000 },
}));

// 5 login attempts per email, then 1 more every 5 minutes.
app.post('/login', kahunaRateLimit<express.Request>({
  client,
  policy: 'login_attempts',
  partitionBy: (req) => String(req.query.email ?? 'unknown'),
  tokenBucket: { tokenLimit: 5, replenishmentPeriodMs: 5 * 60_000, tokensPerPeriod: 1 },
}), login);
```

- A refused request gets status 429. It also gets a `Retry-After` header when the cluster knows when the budget frees up.
- `rejectionStatusCode` and `onRejected` change the refusal response.
- A concurrency permit comes back when the response closes.
- The key is `rate-limit/<policy>`, or `rate-limit/<policy>/<partition>` with `partitionBy`. The .NET package uses the same keys.
- Set `keyPrefix`, or pass a settings function, to use other keys.
- Wrong settings throw when you build the middleware, not on the first request.
- Close the policy when the application stops: `await middleware.policy.close()`.

### Frameworks

| Framework | Use |
| --- | --- |
| Express, Connect, `node:http` | `kahunaRateLimit` |
| Handlers that take a `Request` and return a `Response` (Hono, Bun, Deno, route handlers) | `withKahunaRateLimit` |
| Any other framework | Build a `KahunaRateLimitPolicy` and call `policy.acquire(request)` from its request hook. |

```ts
import { withKahunaRateLimit } from 'kahuna-client/middleware';

export const POST = withKahunaRateLimit(
  { client, policy: 'reports', concurrency: { permitLimit: 10 } },
  async (request) => Response.json(await generateReport(request)),
);
```
