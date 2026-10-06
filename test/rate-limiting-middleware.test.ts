import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';

import { afterAll, describe, expect, it } from 'vitest';

import type { KahunaClient } from '../src/index.js';
import {
  KahunaRateLimitPolicy,
  kahunaRateLimit,
  withKahunaRateLimit,
  type ConnectMiddleware,
} from '../src/middleware.js';
import { ENDPOINTS, closeClients, makeClient } from './support/cluster.js';

afterAll(closeClients);

type Handler = (request: IncomingMessage, response: ServerResponse) => void | Promise<void>;

interface Replica {
  readonly url: string;
  readonly server: Server;
  readonly middlewares: ConnectMiddleware<IncomingMessage, ServerResponse>[];
}

/** Runs `middleware` and then `handler`, the way Connect and Express chain them. */
function route(middleware: ConnectMiddleware<IncomingMessage, ServerResponse>, handler: Handler): Handler {
  return (request, response) =>
    middleware(request, response, (error) => {
      if (error) {
        response.statusCode = 500;
        response.end(String(error));
        return;
      }
      void handler(request, response);
    });
}

function ok(_request: IncomingMessage, response: ServerResponse): void {
  response.end('ok');
}

/**
 * One application replica: a fixed-window route, a token-bucket route
 * partitioned by a query value, and a concurrency route that signals when it starts
 * and then holds its request until a gate opens. Every replica names the same keys,
 * so the replicas share budgets.
 */
async function startReplica(
  client: KahunaClient,
  prefix: string,
  entered: () => void,
  gate: Promise<void>,
): Promise<Replica> {
  const simple = kahunaRateLimit({
    client,
    policy: 'simple_endpoints',
    keyPrefix: prefix,
    fixedWindow: { permitLimit: 3, windowMs: 3_600_000 },
  });

  const login = kahunaRateLimit({
    client,
    policy: 'login_attempts',
    keyPrefix: prefix,
    partitionBy: (request) => new URL(request.url ?? '/', 'http://localhost').searchParams.get('email') ?? '',
    tokenBucket: { tokenLimit: 2, replenishmentPeriodMs: 3_600_000, tokensPerPeriod: 1 },
  });

  const expensive = kahunaRateLimit({
    client,
    policy: 'expensive_operations',
    keyPrefix: prefix,
    concurrency: { permitLimit: 1 },
  });

  const routes: Record<string, Handler> = {
    '/simple': route(simple, ok),
    '/login': route(login, ok),
    '/expensive': route(expensive, async (_request, response) => {
      entered();
      await gate;
      response.end('ok');
    }),
  };

  const server = createServer((request, response) => {
    const path = new URL(request.url ?? '/', 'http://localhost').pathname;
    const handler = routes[path];
    if (handler) return void handler(request, response);
    response.statusCode = 404;
    response.end();
  });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;

  return { url: `http://127.0.0.1:${port}`, server, middlewares: [simple, login, expensive] };
}

async function stopReplica(replica: Replica): Promise<void> {
  replica.server.closeAllConnections();
  await new Promise<void>((resolve) => replica.server.close(() => resolve()));
  await Promise.all(replica.middlewares.map((middleware) => middleware.policy.close()));
}

/** Ported from `Kahuna.Server.Tests/TestKahunaRateLimitingMiddleware.cs`. */
describe('rate-limiting middleware', () => {
  it('shares budgets between replicas', async () => {
    const prefix = `rate-limit/web/${crypto.randomUUID().slice(0, 8)}/`;

    let signalEntered!: () => void;
    const entered = new Promise<void>((resolve) => (signalEntered = resolve));
    let openGate!: () => void;
    const gate = new Promise<void>((resolve) => (openGate = resolve));

    const replica1 = await startReplica(makeClient('grpc', 'single', { endpoints: ENDPOINTS[0]! }), prefix, signalEntered, gate);
    const replica2 = await startReplica(
      makeClient('rest', 'single', { endpoints: ENDPOINTS[1 % ENDPOINTS.length]! }),
      prefix,
      signalEntered,
      gate,
    );

    try {
      // Fixed window: three requests per window, spread over two replicas.
      const simple: Response[] = [];
      for (let i = 0; i < 6; i++) simple.push(await fetch(`${(i % 2 === 0 ? replica1 : replica2).url}/simple`));

      expect(simple.filter((response) => response.status === 200)).toHaveLength(3);
      const refused = simple.filter((response) => response.status === 429);
      expect(refused).toHaveLength(3);
      for (const response of refused) expect(Number(response.headers.get('retry-after'))).toBeGreaterThanOrEqual(1);

      // Token bucket per email: each email has its own two tokens.
      let alice = 0;
      let bob = 0;
      for (let i = 0; i < 4; i++) {
        if ((await fetch(`${replica1.url}/login?email=alice@example.com`)).status === 200) alice++;
        if ((await fetch(`${replica2.url}/login?email=bob@example.com`)).status === 200) bob++;
      }
      expect(alice).toBe(2);
      expect(bob).toBe(2);

      // Concurrency: one request in progress across both replicas. The first holds
      // the permit until the gate opens, so a second one on the other replica is refused.
      const holding = fetch(`${replica1.url}/expensive`);
      await entered;

      expect((await fetch(`${replica2.url}/expensive`)).status).toBe(429);

      openGate();
      expect((await holding).status).toBe(200);

      // The permit comes back when the first request ends, so the other replica gets it.
      let after = 429;
      for (let i = 0; i < 100 && after !== 200; i++) {
        after = (await fetch(`${replica2.url}/expensive`)).status;
        if (after !== 200) await new Promise((resolve) => setTimeout(resolve, 20));
      }
      expect(after).toBe(200);
    } finally {
      openGate();
      await stopReplica(replica1);
      await stopReplica(replica2);
    }
  });

  it('wraps a Fetch-style handler', async () => {
    const client = makeClient('grpc', 'pool');

    const handler = withKahunaRateLimit(
      {
        client,
        policy: `fetch-${crypto.randomUUID().slice(0, 8)}`,
        partitionBy: (request) => request.headers.get('x-user') ?? 'anonymous',
        slidingWindow: { permitLimit: 2, windowMs: 3_600_000, segmentsPerWindow: 4 },
      },
      () => new Response('ok'),
    );

    try {
      const request = (user: string): Request => new Request('http://localhost/', { headers: { 'x-user': user } });

      const statuses: number[] = [];
      for (let i = 0; i < 3; i++) statuses.push((await handler(request('carol'))).status);
      expect(statuses).toEqual([200, 200, 429]);

      // Another partition has its own budget.
      expect((await handler(request('dave'))).status).toBe(200);
    } finally {
      await handler.policy.close();
    }
  });

  it('checks the policy settings when it is built', () => {
    const client = makeClient('grpc', 'pool');

    expect(
      () => new KahunaRateLimitPolicy({ client, policy: 'bad', fixedWindow: { permitLimit: 0, windowMs: 1000 } }),
    ).toThrow(RangeError);

    expect(() => new KahunaRateLimitPolicy({ client, policy: '', concurrency: { permitLimit: 1 } })).toThrow(TypeError);

    expect(
      () =>
        new KahunaRateLimitPolicy({
          client,
          policy: 'both',
          fixedWindow: { permitLimit: 1, windowMs: 1000 },
          concurrency: { permitLimit: 1 },
        } as never),
    ).toThrow(TypeError);
  });

  it('names one key per partition', () => {
    const client = makeClient('grpc', 'pool');
    const keys: string[] = [];

    const policy = new KahunaRateLimitPolicy<string>({
      client,
      policy: 'login_attempts',
      partitionBy: (email) => email,
      tokenBucket: (_partition, defaultKey) => {
        keys.push(defaultKey);
        return { tokenLimit: 5, replenishmentPeriodMs: 1000, tokensPerPeriod: 1 };
      },
    });

    expect(policy.limiterFor('alice').key).toBe('rate-limit/login_attempts/alice');
    expect(keys).toEqual(['rate-limit/login_attempts/partition', 'rate-limit/login_attempts/alice']);
  });
});
