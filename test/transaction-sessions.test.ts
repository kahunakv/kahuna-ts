import { afterAll, describe, expect, it } from 'vitest';

import {
  closeClients,
  combinations,
  makeClient,
  randomKey,
  sleep,
  type ClientShape,
  type TransportKind,
} from './support/cluster.js';

afterAll(closeClients);

/** Ported from `Kahuna.Client.Tests/TestTransactionSessions.cs`. */
describe.each(combinations())(
  'transaction sessions over %s, %s endpoint',
  (transport: TransportKind, shape: ClientShape) => {
    it('makes a committed write visible outside the transaction', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      const session = await client.beginTransaction({ locking: 'optimistic' });
      const written = await session.set(key, 'transaction-value', { expiry: 10_000 });
      expect(written.success).toBe(true);

      expect(await session.commit()).toBe(true);
      expect(session.status).toBe('committed');

      const read = await client.get(key);
      expect(read.success).toBe(true);
      expect(read.valueAsString()).toBe('transaction-value');
    });

    it('leaves no trace of a rolled-back write', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      const session = await client.beginTransaction({ locking: 'optimistic' });
      expect((await session.set(key, 'transaction-value', { expiry: 10_000 })).success).toBe(true);

      expect(await session.rollback()).toBe(true);
      expect(session.status).toBe('rolledBack');

      expect((await client.get(key)).success).toBe(false);
    });

    it('applies writes and deletes of one transaction together', async () => {
      const client = makeClient(transport, shape);
      const [first, second, third] = [randomKey(), randomKey(), randomKey()];

      const session = await client.beginTransaction({ locking: 'optimistic' });
      expect((await session.set(first, 'value1', { expiry: 10_000 })).success).toBe(true);
      expect((await session.set(second, 'value2', { expiry: 10_000 })).success).toBe(true);
      expect((await session.set(third, 'value3', { expiry: 10_000 })).success).toBe(true);
      expect((await session.delete(second)).success).toBe(true);

      await session.commit();

      expect((await client.get(first)).valueAsString()).toBe('value1');
      expect((await client.get(second)).success).toBe(false);
      expect((await client.get(third)).valueAsString()).toBe('value3');
    });

    it('reads and updates one key under pessimistic locking', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      expect((await client.set(key, 'initial-value', { expiry: 10_000 })).success).toBe(true);

      const session = await client.beginTransaction({ locking: 'pessimistic' });

      const read = await session.get(key);
      expect(read.success).toBe(true);
      expect(read.valueAsString()).toBe('initial-value');

      expect((await session.set(key, 'updated-value', { expiry: 10_000 })).success).toBe(true);
      await session.commit();

      expect((await client.get(key)).valueAsString()).toBe('updated-value');
    });

    it('deletes several keys inside one transaction', async () => {
      const client = makeClient(transport, shape);
      const keys = [randomKey(), randomKey(), randomKey()];

      for (const key of keys) await client.set(key, 'value', { expiry: 10_000 });

      const session = await client.beginTransaction({ locking: 'optimistic' });
      const deleted = await session.deleteMany(keys);
      expect(deleted.every((result) => result.success)).toBe(true);
      await session.commit();

      for (const key of keys) expect((await client.get(key)).success).toBe(false);
    });

    it('runs a transaction body once when nothing conflicts', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();
      let attempts = 0;

      await client.withTransaction({ locking: 'optimistic' }, async (session) => {
        attempts++;
        await session.set(key, `value-${attempts}`, { expiry: 10_000 });
        await session.commit();
      });

      expect(attempts).toBe(1);
      expect((await client.get(key)).valueAsString()).toBe('value-1');
    });

    it('rolls a pending session back when it is disposed', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      {
        await using session = await client.beginTransaction({ locking: 'optimistic' });
        await session.set(key, 'never-committed', { expiry: 10_000 });
      }

      expect((await client.get(key)).success).toBe(false);
    });

    it('refuses work after the transaction is finalized', async () => {
      const client = makeClient(transport, shape);

      const session = await client.beginTransaction({ locking: 'optimistic' });
      await session.commit();

      await expect(session.set(randomKey(), 'late', { expiry: 10_000 })).rejects.toThrow(
        /completed transaction/,
      );
      await expect(session.commit()).rejects.toThrow(/not pending/);
    });

    it('leaves the session lifetime to the server rather than expiring it locally', async () => {
      const client = makeClient(transport, shape);

      const session = await client.beginTransaction({ locking: 'optimistic', timeout: 1 });
      await sleep(100);

      // The timeout governs the server's transaction lifecycle. The client does not
      // refuse work on its own clock.
      const written = await session.set(randomKey(), 'value', { expiry: 10_000 });
      expect(written.success).toBe(true);

      await session.rollback();
    });

    it('reads a bucket inside a transaction', async () => {
      const client = makeClient(transport, shape);
      const prefix = randomKey('tx-bucket');

      for (let index = 0; index < 3; index++) {
        await client.set(`${prefix}/key-${index}`, `value-${index}`, { expiry: 10_000 });
      }
      await sleep(500);

      await using session = await client.beginTransaction({ locking: 'optimistic' });
      const items = await session.getByBucket(prefix);

      expect(items).toHaveLength(3);
      await session.commit();
    });
  },
);
