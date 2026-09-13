import { afterAll, describe, expect, it } from 'vitest';

import { KahunaClient } from '../src/index.js';
import {
  ENDPOINTS,
  SINGLE_ENDPOINT,
  TRANSPORTS,
  closeClients,
  makeClient,
  randomKey,
  type TransportKind,
} from './support/cluster.js';

afterAll(closeClients);

/**
 * Every node advertises the address its peers route on, which is not always the
 * address this client dials. The map covers both layouts the suite runs against:
 * the compose network puts the advertised address on a container IP, and a local
 * cluster puts it on the loopback literal while the tests dial the host name. An
 * entry for a layout that is not in use costs nothing.
 */
const ENDPOINT_MAP: Record<string, string> = {
  'https://172.30.0.2:8082': 'https://localhost:8082',
  'https://172.30.0.3:8084': 'https://localhost:8084',
  'https://172.30.0.4:8086': 'https://localhost:8086',
  'https://127.0.0.1:8082': 'https://localhost:8082',
  'https://127.0.0.1:8084': 'https://localhost:8084',
  'https://127.0.0.1:8086': 'https://localhost:8086',
};

/** Ported from `Kahuna.Client.Tests/TestClientRouting.cs`. */
describe.each(TRANSPORTS)('routing over %s', (transport: TransportKind) => {
  function routedClient(routing: 'learned' | 'metadata' | 'roundRobin', withMap = true) {
    return makeClient(transport, 'pool', {
      routing,
      routingEndpointMap: withMap ? ENDPOINT_MAP : undefined,
    });
  }

  it('routes a repeated key to the node that served it', async () => {
    const client = routedClient('learned');
    const key = `routing/${randomKey()}`;

    expect((await client.set(key, 'v1', { expiry: 10_000 })).success).toBe(true);

    expect(client.router).not.toBeNull();
    const learned = client.router!.select('keyValue', key);
    expect(learned).not.toBeNull();
    expect(ENDPOINTS).toContain(learned);

    const read = await client.get(key);
    expect(read.success).toBe(true);
    expect(read.valueAsString()).toBe('v1');
    expect(client.router!.select('keyValue', key)).toBe(learned);
  });

  it('keeps a lock and a key of the same name on separate routes', async () => {
    const client = routedClient('learned');
    const name = `routing/${randomKey()}`;

    expect((await client.set(name, 'v', { expiry: 10_000 })).success).toBe(true);

    await using held = await client.acquireLock(name, { expiry: 10_000 });
    expect(held.acquired).toBe(true);

    // A lock and a key of one name may live on different partitions, so learning one
    // must never answer for the other.
    expect(client.router!.select('keyValue', name)).not.toBeNull();
    expect(client.router!.select('lock', name)).not.toBeNull();
  });

  it('refuses hints it cannot dial, and keeps working without them', async () => {
    const client = routedClient('learned', false);
    const key = `routing/${randomKey()}`;

    expect((await client.set(key, 'v', { expiry: 10_000 })).success).toBe(true);

    const read = await client.get(key);
    expect(read.success).toBe(true);
    expect(read.valueAsString()).toBe('v');

    // Without a map every hint names an address this client cannot reach, so nothing
    // is learned and the client stays on endpoint rotation.
    expect(client.router!.cachedRouteCount).toBe(0);
  });

  it('resolves a key it has never touched once the map is read', async () => {
    const client = routedClient('metadata');

    expect(await client.router!.refreshMetadata()).toBe(true);

    let resolved = 0;
    for (let index = 0; index < 20; index++) {
      if (client.router!.select('keyValue', `routing/unseen/${randomKey()}`) !== null) resolved++;
    }

    // Every partition of a settled cluster has a known leader, so almost every
    // unseen key resolves.
    expect(resolved).toBeGreaterThanOrEqual(18);
  });

  it('keeps rotating endpoints when the caller asks for rotation', async () => {
    const client = routedClient('roundRobin');

    expect(client.routing).toBe('roundRobin');
    expect(client.router).toBeNull();

    const key = `routing/${randomKey()}`;
    expect((await client.set(key, 'v', { expiry: 10_000 })).success).toBe(true);
    expect((await client.get(key)).valueAsString()).toBe('v');
  });

  // The default resolves against the number of endpoints, so this case needs a
  // cluster rather than the single standalone node.
  it.skipIf(ENDPOINTS.length < 2)('learns without being asked when it has several endpoints', async () => {
    const client = makeClient(transport, 'pool', { routingEndpointMap: ENDPOINT_MAP });

    expect(client.routing).toBe('learned');
    expect(client.router).not.toBeNull();

    const key = `routing/${randomKey()}`;
    expect((await client.set(key, 'v', { expiry: 10_000 })).success).toBe(true);

    const learned = client.router!.select('keyValue', key);
    expect(learned).not.toBeNull();
    expect(ENDPOINTS).toContain(learned);
  });

  it('stays on rotation when it has a single endpoint', async () => {
    const client = new KahunaClient({
      endpoints: SINGLE_ENDPOINT,
      transport,
      allowInsecureCertificateValidation: true,
    });

    try {
      expect(client.routing).toBe('roundRobin');
      expect(client.router).toBeNull();

      const key = `routing/${randomKey()}`;
      expect((await client.set(key, 'v', { expiry: 10_000 })).success).toBe(true);
      expect((await client.get(key)).valueAsString()).toBe('v');
    } finally {
      await client.close();
    }
  });

  it('leaves the semantics of a lock unchanged under routing', async () => {
    const client = routedClient('learned');
    const resource = `routing/lock/${randomKey()}`;

    await using held = await client.acquireLock(resource, { expiry: 10_000 });
    expect(held.acquired).toBe(true);
    expect(held.fencingToken).toBeGreaterThanOrEqual(0);

    const extended = await held.extend(10_000);
    expect(extended.extended).toBe(true);
    expect(extended.fencingToken).toBe(held.fencingToken);

    const info = await held.info();
    expect(info).not.toBeNull();
    expect(new TextDecoder().decode(info!.owner!)).toBe(held.tokenAsString);
  });

  it('warms the route of every key a batched write touched', async () => {
    const client = routedClient('learned');
    const keys: string[] = [];

    const items = Array.from({ length: 10 }, (_, index) => {
      const key = `routing/batch${index}/${randomKey()}`;
      keys.push(key);
      return { key, value: 'v', expiry: 10_000, durability: 'persistent' as const };
    });

    const written = await client.setMany(items);
    expect(written.every((result) => result.success)).toBe(true);

    // Each row carries its own route index, so the point operations that follow go
    // direct instead of each discovering its own key first.
    for (const key of keys) {
      expect(client.router!.select('keyValue', key)).not.toBeNull();
    }
  });
});
