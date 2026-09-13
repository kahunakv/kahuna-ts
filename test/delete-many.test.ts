import { afterAll, describe, expect, it } from 'vitest';

import {
  closeClients,
  combinations,
  makeClient,
  randomKey,
  type ClientShape,
  type TransportKind,
} from './support/cluster.js';

afterAll(closeClients);

/** Ported from `Kahuna.Client.Tests/TestDeleteManyKeyValues.cs`. */
describe.each(combinations())(
  'batched deletes over %s, %s endpoint',
  (transport: TransportKind, shape: ClientShape) => {
    it.each(['persistent', 'ephemeral'] as const)('deletes %s keys', async (durability) => {
      const client = makeClient(transport, shape);
      const prefix = randomKey();
      const keys: string[] = [];

      for (let i = 0; i < 3; i++) {
        const key = `${prefix}-${durability}-${i}`;
        keys.push(key);
        expect((await client.set(key, `value-${i}`, { expiry: 10_000, durability })).success).toBe(
          true,
        );
      }

      const deleted = await client.deleteMany(keys, { durability });

      expect(deleted).toHaveLength(keys.length);
      expect(deleted.every((result) => result.success)).toBe(true);
      expect(deleted.every((result) => result.durability === durability)).toBe(true);

      for (const key of keys) {
        expect((await client.get(key, { durability })).success).toBe(false);
      }
    });

    it('deletes keys of mixed durability in one request', async () => {
      const client = makeClient(transport, shape);
      const persistentKey = randomKey();
      const ephemeralKey = randomKey();

      await client.set(persistentKey, 'persistent-value', {
        expiry: 10_000,
        durability: 'persistent',
      });
      await client.set(ephemeralKey, 'ephemeral-value', {
        expiry: 10_000,
        durability: 'ephemeral',
      });

      const deleted = await client.deleteMany([
        { key: persistentKey, durability: 'persistent' },
        { key: ephemeralKey, durability: 'ephemeral' },
      ]);

      expect(deleted).toHaveLength(2);
      expect(deleted.every((result) => result.success)).toBe(true);

      expect((await client.get(persistentKey, { durability: 'persistent' })).success).toBe(false);
      expect((await client.get(ephemeralKey, { durability: 'ephemeral' })).success).toBe(false);
    });

    it('reports per key whether that key existed', async () => {
      const client = makeClient(transport, shape);
      const existing = randomKey();
      const missing = randomKey();

      await client.set(existing, 'value', { expiry: 10_000 });

      const deleted = await client.deleteMany([existing, missing]);

      expect(deleted).toHaveLength(2);
      expect(deleted.find((result) => result.key === existing)?.success).toBe(true);
      expect(deleted.find((result) => result.key === missing)?.success).toBe(false);
      expect((await client.get(existing)).success).toBe(false);
    });

    it('rejects one invalid key without failing the whole batch', async () => {
      const client = makeClient(transport, shape);
      const valid = randomKey();

      await client.set(valid, 'value', { expiry: 10_000 });

      const deleted = await client.deleteMany(['', valid]);

      expect(deleted).toHaveLength(2);
      // The empty key is refused on its own row, so the valid key is still deleted.
      expect(deleted.find((result) => result.key === '')?.success).toBe(false);
      expect(deleted.find((result) => result.key === valid)?.success).toBe(true);
      expect((await client.get(valid)).success).toBe(false);
    });

    it('names the refusal code of each rejected key', async () => {
      const client = makeClient(transport, shape);
      const valid = randomKey();

      await client.set(valid, 'value', { expiry: 10_000 });

      // The per-key code lives on the transport result; the client maps it to the
      // success flag an entry carries.
      const outcome = await client.transport.deleteMany(client.endpoints[0]!, [
        { key: '', durability: 'persistent' },
        { key: valid, durability: 'persistent' },
      ]);

      expect(outcome.items).toHaveLength(2);
      expect(outcome.items.find((item) => item.key === '')?.type).toBe('invalidInput');
      expect(outcome.items.find((item) => item.key === valid)?.type).toBe('deleted');
    });
  },
);
