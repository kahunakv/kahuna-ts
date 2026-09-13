import { afterAll, describe, expect, it } from 'vitest';

import type { Durability } from '../src/index.js';
import {
  closeClients,
  durableCombinations,
  makeClient,
  randomLockName,
  type ClientShape,
  type TransportKind,
} from './support/cluster.js';

afterAll(closeClients);

/** Ported from `Kahuna.Client.Tests/TestLocks.cs`. */
describe.each(durableCombinations())(
  'locks over %s, %s endpoint, %s durability',
  (transport: TransportKind, shape: ClientShape, durability: Durability) => {
    it('acquires a free lock and starts its fencing token at zero', async () => {
      const client = makeClient(transport, shape);
      const name = randomLockName();

      await using lock = await client.acquireLock(name, { expiry: 1_000, durability });

      expect(lock.acquired).toBe(true);
      expect(lock.fencingToken).toBe(0);
    });

    it('refuses a second holder, and admits one again after the first releases', async () => {
      const client = makeClient(transport, shape);
      const name = randomLockName();

      const first = await client.acquireLock(name, { expiry: 10_000, durability });
      const second = await client.acquireLock(name, { expiry: 10_000, durability });

      expect(first.acquired).toBe(true);
      expect(first.fencingToken).toBe(0);
      expect(second.acquired).toBe(false);

      await first.release();
      await second.release();

      // The fencing token advances with every acquisition, so a holder that woke up
      // late can be told apart from the current one.
      await using third = await client.acquireLock(name, { expiry: 1_000, durability });
      expect(third.acquired).toBe(true);
      expect(third.fencingToken).toBe(1);
    });

    it('waits for a lock when the caller asks it to', async () => {
      const client = makeClient(transport, shape);
      const name = randomLockName();

      const first = await client.acquireLock(name, {
        expiry: 1_000,
        wait: 1_000,
        retry: 500,
        durability,
      });

      expect(first.acquired).toBe(true);
      expect(first.fencingToken).toBe(0);
      await first.release();

      await using second = await client.acquireLock(name, { expiry: 1_000, durability });
      expect(second.acquired).toBe(true);
      expect(second.fencingToken).toBe(1);
    });

    it('refuses a wait with no retry interval', async () => {
      const client = makeClient(transport, shape);
      await expect(
        client.acquireLock(randomLockName(), { expiry: 1_000, wait: 1_000, durability }),
      ).rejects.toThrow('Retry cannot be zero');
    });

    it('extends a held lock and keeps its fencing token', async () => {
      const client = makeClient(transport, shape);
      const name = randomLockName();

      await using lock = await client.acquireLock(name, { expiry: 10_000, durability });
      expect(lock.acquired).toBe(true);

      const first = await lock.extend(10_000);
      expect(first.extended).toBe(true);
      expect(first.fencingToken).toBe(lock.fencingToken);

      const info = await client.getLockInfo(name, { durability });
      expect(info).not.toBeNull();
      expect(Buffer.from(info!.owner!)).toEqual(Buffer.from(lock.token));

      const before = info!.expires;
      const second = await lock.extend(20_000);
      expect(second.extended).toBe(true);

      const after = await client.getLockInfo(name, { durability });
      expect(after).not.toBeNull();
      expect(after!.expires.physical).toBeGreaterThan(before.physical);
    });

    it('reads its own information through the handle', async () => {
      const client = makeClient(transport, shape);
      const name = randomLockName();

      await using lock = await client.acquireLock(name, { expiry: 10_000, durability });
      expect(lock.acquired).toBe(true);

      const info = await lock.info();
      expect(info).not.toBeNull();
      expect(Buffer.from(info!.owner!)).toEqual(Buffer.from(lock.token));
      expect(info!.fencingToken).toBe(lock.fencingToken);
    });

    it('gives exactly one of many racing callers each distinct lock', async () => {
      const client = makeClient(transport, shape);

      await Promise.all(
        Array.from({ length: 10 }, async () => {
          const name = randomLockName();
          const first = await client.acquireLock(name, {
            expiry: 5_000,
            wait: 5_000,
            retry: 500,
            durability,
          });

          expect(first.acquired).toBe(true);
          expect(first.fencingToken).toBe(0);
          await first.release();

          await using second = await client.acquireLock(name, { expiry: 5_000, durability });
          expect(second.acquired).toBe(true);
          expect(second.fencingToken).toBe(1);
        }),
      );
    });

    it('serialises ten callers that all wait for one lock', async () => {
      const client = makeClient(transport, shape);
      const name = randomLockName();
      let held = 0;

      await Promise.all(
        Array.from({ length: 10 }, async () => {
          await using lock = await client.acquireLock(name, {
            expiry: 10_000,
            wait: 11_000,
            retry: 500,
            durability,
            signal: AbortSignal.timeout(20_000),
          });

          if (lock.acquired) held++;
        }),
      );

      // Every caller waited long enough, so every caller eventually held the lock.
      expect(held).toBe(10);
    });
  },
);
