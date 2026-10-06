import { afterAll, describe, expect, it } from 'vitest';

import {
  KahunaClient,
  KahunaConcurrencyLimiter,
  KahunaFixedWindowRateLimiter,
  KahunaPartitionedRateLimiter,
  KahunaSlidingWindowRateLimiter,
  KahunaTokenBucketRateLimiter,
  OperationAbortedError,
  RateLimitLease,
  rateLimitKeyFor,
  type KahunaRateLimiter,
  type RateLimiterFailureMode,
  type Transport,
} from '../src/index.js';
import { ENDPOINTS, TRANSPORTS, closeClients, makeClient, type TransportKind } from './support/cluster.js';

afterAll(closeClients);

const DURABILITIES = ['ephemeral', 'persistent'] as const;

const HOUR = 3_600_000;

function newKey(): string {
  return `rate-limit/test/${crypto.randomUUID().replaceAll('-', '').slice(0, 12)}`;
}

/** One client per node, each standing for one replica of an application. */
function replicas(transport: TransportKind): KahunaClient[] {
  return [0, 1, 2].map((i) =>
    makeClient(transport, 'single', { endpoints: ENDPOINTS[i % ENDPOINTS.length]! }),
  );
}

async function acquireConcurrently(
  limiters: readonly KahunaRateLimiter[],
  requests: number,
): Promise<{ granted: number; refused: number; leases: RateLimitLease[] }> {
  const leases = await Promise.all(
    Array.from({ length: requests }, (_, i) => limiters[i % limiters.length]!.acquire(1)),
  );
  const granted = leases.filter((lease) => lease.acquired).length;
  return { granted, refused: requests - granted, leases };
}

function retryAfter(lease: RateLimitLease): number {
  expect(lease.acquired).toBe(false);
  expect(lease.retryAfterMs, 'A refusal should say when to retry').not.toBeNull();
  return lease.retryAfterMs!;
}

async function waitForGrant(limiter: KahunaRateLimiter): Promise<RateLimitLease> {
  for (let i = 0; i < 100; i++) {
    const lease = await limiter.acquire(1);
    if (lease.acquired) return lease;
    await delay(20);
  }
  return limiter.acquire(1);
}

async function waitUntil(condition: () => boolean): Promise<void> {
  for (let i = 0; i < 500 && !condition(); i++) await delay(10);
  expect(condition()).toBe(true);
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Ported from `Kahuna.Server.Tests/TestKahunaRateLimiters.cs`. */
describe.each(TRANSPORTS)('rate limiters over %s', (transport: TransportKind) => {
  it.each(DURABILITIES)('shares a %s fixed window across replicas', async (durability) => {
    const key = newKey();

    // An hour-long window, so the test cannot straddle a window boundary.
    const limiters = replicas(transport).map(
      (client) => new KahunaFixedWindowRateLimiter(client, { key, durability, permitLimit: 5, windowMs: HOUR }),
    );

    let granted = 0;

    for (let i = 0; i < 12; i++) {
      const lease = await limiters[i % limiters.length]!.acquire(1);

      if (lease.acquired) {
        granted++;
        continue;
      }

      const wait = retryAfter(lease);
      expect(wait).toBeGreaterThanOrEqual(1);
      expect(wait).toBeLessThanOrEqual(HOUR);
    }

    expect(granted).toBe(5);
  });

  it('holds a fixed window under contention', async () => {
    const key = newKey();
    const limiters = replicas(transport).map(
      (client) => new KahunaFixedWindowRateLimiter(client, { key, permitLimit: 5, windowMs: HOUR, maxRetries: 100 }),
    );

    const { granted, refused } = await acquireConcurrently(limiters, 20);

    expect(granted).toBe(5);
    expect(refused).toBe(15);
  });

  it('resets a fixed window after the retry-after', async () => {
    const clients = replicas(transport);
    const options = { key: newKey(), permitLimit: 2, windowMs: 1000 };

    const first = new KahunaFixedWindowRateLimiter(clients[0]!, options);
    const second = new KahunaFixedWindowRateLimiter(clients[1]!, options);

    // Spend the window, whichever window it is. A spent window refuses.
    let lease: RateLimitLease;
    do lease = await first.acquire(1);
    while (lease.acquired);

    const wait = retryAfter(lease);
    expect(wait).toBeGreaterThanOrEqual(1);
    expect(wait).toBeLessThanOrEqual(1000);

    await delay(wait + 50);

    expect((await second.acquire(1)).acquired).toBe(true);
    expect((await first.acquire(1)).acquired).toBe(true);
  });

  it('returns sliding-window permits segment by segment', async () => {
    const clients = replicas(transport);
    const options = { key: newKey(), permitLimit: 4, windowMs: 2000, segmentsPerWindow: 4 };

    const first = new KahunaSlidingWindowRateLimiter(clients[0]!, options);
    const second = new KahunaSlidingWindowRateLimiter(clients[1]!, options);
    const third = new KahunaSlidingWindowRateLimiter(clients[2]!, options);

    expect((await first.acquire(2)).acquired).toBe(true);

    await delay(1100);

    expect((await second.acquire(2)).acquired).toBe(true);

    const wait = retryAfter(await third.acquire(1));
    expect(wait).toBeGreaterThanOrEqual(1);
    expect(wait).toBeLessThanOrEqual(1000);

    await delay(wait + 50);

    expect((await third.acquire(2)).acquired).toBe(true);

    // The later two permits are still in the window.
    expect(retryAfter(await first.acquire(1))).toBeGreaterThan(0);
  });

  it('holds a sliding window under contention', async () => {
    const key = newKey();
    const limiters = replicas(transport).map(
      (client) =>
        new KahunaSlidingWindowRateLimiter(client, {
          key,
          permitLimit: 5,
          windowMs: HOUR,
          segmentsPerWindow: 10,
          maxRetries: 100,
        }),
    );

    expect((await acquireConcurrently(limiters, 20)).granted).toBe(5);
  });

  it('bursts a token bucket, then refills it one period at a time', async () => {
    const options = { key: newKey(), tokenLimit: 3, replenishmentPeriodMs: 1000, tokensPerPeriod: 1 };
    const limiters = replicas(transport).map((client) => new KahunaTokenBucketRateLimiter(client, options));

    for (let i = 0; i < 3; i++) expect((await limiters[i]!.acquire(1)).acquired).toBe(true);

    const wait = retryAfter(await limiters[0]!.acquire(1));
    expect(wait).toBeGreaterThanOrEqual(1);
    expect(wait).toBeLessThanOrEqual(1000);

    await delay(wait + 50);

    expect((await limiters[1]!.acquire(1)).acquired).toBe(true);
    expect((await limiters[2]!.acquire(1)).acquired).toBe(false);
  });

  it('holds a token bucket under contention', async () => {
    const key = newKey();
    const limiters = replicas(transport).map(
      (client) =>
        new KahunaTokenBucketRateLimiter(client, {
          key,
          tokenLimit: 5,
          replenishmentPeriodMs: HOUR,
          tokensPerPeriod: 1,
          maxRetries: 100,
        }),
    );

    expect((await acquireConcurrently(limiters, 20)).granted).toBe(5);
  });

  it.each(DURABILITIES)('shares %s concurrency permits and returns them on release', async (durability) => {
    const key = newKey();
    const limiters = replicas(transport).map(
      (client) => new KahunaConcurrencyLimiter(client, { key, durability, permitLimit: 4, maxRetries: 100 }),
    );

    const { granted, leases } = await acquireConcurrently(limiters, 12);
    expect(granted).toBe(4);

    expect((await limiters[0]!.acquire(1)).acquired).toBe(false);

    leases.find((lease) => lease.acquired)!.release();

    // The release runs in the background. It is a single-key script, so it lands quickly.
    const next = await waitForGrant(limiters[2]!);
    expect(next.acquired).toBe(true);

    expect((await limiters[1]!.acquire(1)).acquired).toBe(false);

    for (const lease of leases) lease.release();
    next.release();

    await Promise.all(limiters.map((limiter) => limiter.close()));
  });

  it('renews a held concurrency lease and lets an abandoned one expire', async () => {
    const clients = replicas(transport);
    const options = { key: newKey(), permitLimit: 1, leaseDurationMs: 600 };

    const holder = new KahunaConcurrencyLimiter(clients[0]!, options);
    const other = new KahunaConcurrencyLimiter(clients[1]!, options);

    const held = await holder.acquire(1);
    expect(held.acquired).toBe(true);

    // Three lease durations: without renewal the permit would have come back long ago.
    await delay(1800);
    expect((await other.acquire(1)).acquired).toBe(false);

    // The holder stops renewing, and its lease is never released.
    void holder.close();
    await delay(1200);

    expect((await other.acquire(1)).acquired).toBe(true);
  });

  it('serves the newest concurrency waiter first and evicts the oldest', async () => {
    const [client] = replicas(transport);

    await using limiter = new KahunaConcurrencyLimiter(client!, {
      key: newKey(),
      permitLimit: 1,
      queueLimit: 1,
      queueProcessingOrder: 'newestFirst',
      // Long enough that only the local release, not the poll, can serve the queue in time.
      queuePollIntervalMs: 30_000,
    });

    const held = await limiter.acquire(1);
    expect(held.acquired).toBe(true);

    const oldest = limiter.acquire(1);
    await waitUntil(() => limiter.getStatistics().currentQueuedCount === 1);

    let newestSettled = false;
    const newest = limiter.acquire(1).finally(() => (newestSettled = true));

    expect((await oldest).acquired).toBe(false);
    expect(newestSettled).toBe(false);

    held.release();

    const served = await newest;
    expect(served.acquired).toBe(true);
    served.release();
  });

  it('queues a fixed-window request for the next window and honours cancellation', async () => {
    const clients = replicas(transport);
    const key = newKey();

    await using limiter = new KahunaFixedWindowRateLimiter(clients[0]!, {
      key,
      permitLimit: 1,
      windowMs: 3000,
      queueLimit: 2,
    });

    // Another replica, with no queue, spends the current window.
    await using spender = new KahunaFixedWindowRateLimiter(clients[1]!, { key, permitLimit: 1, windowMs: 3000 });

    // Stop with at least a second left in the window, so the queued request below
    // is refused before the window rolls over.
    while (true) {
      const spent = await spender.acquire(1);
      if (spent.acquired) continue;

      const left = retryAfter(spent);
      if (left >= 1000) break;

      await delay(left + 50);
    }

    const cancelled = new AbortController();
    const abandoned = limiter.acquire(1, { signal: cancelled.signal });

    await waitUntil(() => limiter.getStatistics().currentQueuedCount === 1);
    cancelled.abort();

    await expect(abandoned).rejects.toBeInstanceOf(OperationAbortedError);
    expect(limiter.getStatistics().currentQueuedCount).toBe(0);

    const queued = await limiter.acquire(1);
    expect(queued.acquired).toBe(true);
  });

  it('keeps separate budgets for separate partitions', async () => {
    const policy = `policy-${crypto.randomUUID().slice(0, 8)}`;

    const partitioned = replicas(transport).map(
      (client) =>
        new KahunaPartitionedRateLimiter(
          (user) =>
            new KahunaFixedWindowRateLimiter(client, {
              key: rateLimitKeyFor(policy, user),
              permitLimit: 2,
              windowMs: HOUR,
            }),
        ),
    );

    let alice = 0;
    let bob = 0;

    for (let i = 0; i < 6; i++) {
      if ((await partitioned[i % 3]!.acquire('alice')).acquired) alice++;
      if ((await partitioned[(i + 1) % 3]!.acquire('bob')).acquired) bob++;
    }

    expect(alice).toBe(2);
    expect(bob).toBe(2);

    await Promise.all(partitioned.map((limiter) => limiter.close()));
  });
});

describe('rate limiters without a cluster', () => {
  /** A transport for a cluster that cannot be reached: every call fails as a dropped connection would. */
  function unreachableClient(): KahunaClient {
    const transport = new Proxy({} as Transport, {
      get: (_target, property) =>
        property === 'then' ? undefined : () => Promise.reject(new Error('Connection refused')),
    });

    return new KahunaClient({ endpoints: 'https://localhost:1', transport });
  }

  it('rejects an oversized request and a missing key', async () => {
    const client = unreachableClient();
    const limiter = new KahunaFixedWindowRateLimiter(client, { key: newKey(), permitLimit: 2, windowMs: 1000 });

    await expect(limiter.acquire(3)).rejects.toBeInstanceOf(RangeError);
    expect(() => new KahunaFixedWindowRateLimiter(client, { key: '', permitLimit: 2, windowMs: 1000 })).toThrow(
      TypeError,
    );
    expect(
      () => new KahunaSlidingWindowRateLimiter(client, { key: newKey(), permitLimit: 2, windowMs: 3, segmentsPerWindow: 4 }),
    ).toThrow(RangeError);
  });

  it.each<RateLimiterFailureMode>(['throw', 'allow', 'deny'])(
    'decides by the %s failure mode when the cluster is unreachable',
    async (failureMode) => {
      await using limiter = new KahunaTokenBucketRateLimiter(unreachableClient(), {
        key: newKey(),
        tokenLimit: 5,
        replenishmentPeriodMs: 1000,
        tokensPerPeriod: 1,
        failureMode,
      });

      switch (failureMode) {
        case 'throw':
          await expect(limiter.acquire(1)).rejects.toThrow('Connection refused');
          break;
        case 'allow':
          expect((await limiter.acquire(1)).acquired).toBe(true);
          break;
        case 'deny':
          expect((await limiter.acquire(1)).acquired).toBe(false);
          break;
      }
    },
  );

  it('refuses every waiter when the limiter closes', async () => {
    const limiter = new KahunaFixedWindowRateLimiter(unreachableClient(), {
      key: newKey(),
      permitLimit: 1,
      windowMs: 1000,
      queueLimit: 1,
      failureMode: 'deny',
    });

    const waiting = limiter.acquire(1);
    await waitUntil(() => limiter.getStatistics().currentQueuedCount === 1);

    await limiter.close();

    expect((await waiting).acquired).toBe(false);
    await expect(limiter.acquire(1)).rejects.toThrow('closed');
  });

  it('closes a partition that stays idle', async () => {
    const client = unreachableClient();
    const partitioned = new KahunaPartitionedRateLimiter(
      (partition) =>
        new KahunaFixedWindowRateLimiter(client, {
          key: rateLimitKeyFor('idle', partition),
          permitLimit: 1,
          windowMs: 1000,
          failureMode: 'allow',
        }),
      { idleTimeoutMs: 50 },
    );

    const first = partitioned.limiterFor('a');
    await partitioned.acquire('a');
    expect(partitioned.size).toBe(1);

    await waitUntil(() => partitioned.size === 0);
    expect(first.closed).toBe(true);

    expect(partitioned.limiterFor('a')).not.toBe(first);
    await partitioned.close();
  });
});
