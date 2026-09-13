import { afterAll, describe, expect, it } from 'vitest';

import { KahunaError, type Durability } from '../src/index.js';
import {
  closeClients,
  durableCombinations,
  makeClient,
  randomKey,
  sleep,
  type ClientShape,
  type TransportKind,
} from './support/cluster.js';

afterAll(closeClients);

/** Ported from `Kahuna.Client.Tests/TestKeyValues.cs`. */
describe.each(durableCombinations())(
  'key/values over %s, %s endpoint, %s durability',
  (transport: TransportKind, shape: ClientShape, durability: Durability) => {
    it('refuses an empty key', async () => {
      const client = makeClient(transport, shape);
      await expect(client.set('', 'some-value', { expiry: 10_000, durability })).rejects.toThrow(
        /Failed to set key\/value/,
      );
    });

    it('writes a string value', async () => {
      const client = makeClient(transport, shape);
      const result = await client.set(randomKey(), 'some-value', { expiry: 10_000, durability });

      expect(result.success).toBe(true);
      expect(result.revision).toBe(0);
    });

    it('writes a byte value', async () => {
      const client = makeClient(transport, shape);
      const value = new TextEncoder().encode('some-value');
      const result = await client.set(randomKey(), value, { expiry: 10_000, durability });

      expect(result.success).toBe(true);
      expect(result.revision).toBe(0);
    });

    it('advances the revision on every write', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      const first = await client.set(key, 'some-value', { expiry: 10_000, durability });
      expect(first.success).toBe(true);
      expect(first.revision).toBe(0);

      const second = await client.set(key, 'some-value', { expiry: 10_000, durability });
      expect(second.success).toBe(true);
      expect(second.revision).toBe(1);
    });

    it('writes if the key does not exist, and refuses the second attempt', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      const first = await client.set(key, 'some-value', {
        expiry: 10_000,
        mode: 'ifNotExists',
        durability,
      });
      expect(first.success).toBe(true);
      expect(first.revision).toBe(0);

      const second = await client.set(key, 'some-value', {
        expiry: 10_000,
        mode: 'ifNotExists',
        durability,
      });
      expect(second.success).toBe(false);
      expect(second.revision).toBe(0);
    });

    it('refuses a write if the key does not exist yet', async () => {
      const client = makeClient(transport, shape);
      const result = await client.set(randomKey(), 'some-value', {
        expiry: 10_000,
        mode: 'ifExists',
        durability,
      });

      expect(result.success).toBe(false);
      // A key that was never written reports revision -1 rather than 0, which is the
      // revision of a key that exists and has been written once.
      expect(result.revision).toBe(-1);
    });

    it('writes if the key exists', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      await client.set(key, 'some-value', { expiry: 10_000, durability });
      const result = await client.set(key, 'some-value', {
        expiry: 10_000,
        mode: 'ifExists',
        durability,
      });

      expect(result.success).toBe(true);
      expect(result.revision).toBe(1);
    });

    it('reads back what it wrote', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      await client.set(key, 'some-value', { expiry: 10_000, durability });
      const read = await client.get(key, { durability });

      expect(read.success).toBe(true);
      expect(read.valueAsString()).toBe('some-value');
      expect(read.revision).toBe(0);
    });

    it('stops returning a value once the entry expires', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      await client.set(key, 'some-value', { expiry: 1_000, durability });
      expect((await client.get(key, { durability })).valueAsString()).toBe('some-value');

      await sleep(1_500);

      const expired = await client.get(key, { durability });
      expect(expired.value).toBeNull();
      expect(expired.revision).toBe(0);
    });

    it('honours a one-millisecond expiry', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      const written = await client.set(key, 'some-value', { expiry: 1, durability });
      expect(written.success).toBe(true);

      await sleep(50);

      expect((await client.get(key, { durability })).value).toBeNull();
    });

    it('extends an entry so it outlives its original expiry', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      await client.set(key, 'some-value', { expiry: 1_000, durability });

      const extended = await client.extend(key, 5_000, { durability });
      expect(extended.success).toBe(true);
      expect(extended.revision).toBe(0);

      await sleep(2_000);

      const read = await client.get(key, { durability });
      expect(read.value).not.toBeNull();
      expect(read.revision).toBe(0);
    });

    it('records a delete as a revision of its own', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      await client.set(key, 'some-value', { expiry: 10_000, durability });

      // The tombstone is a first-class revision (the live revision plus one), which
      // preserves the deleted value's own revision for a read as of an earlier time.
      const deleted = await client.delete(key, { durability });
      expect(deleted.success).toBe(true);
      expect(deleted.revision).toBe(1);

      const afterDelete = await client.get(key, { durability });
      expect(afterDelete.value).toBeNull();
      expect(afterDelete.revision).toBe(0);

      const rewritten = await client.set(key, 'some-value-2', { expiry: 10_000, durability });
      expect(rewritten.success).toBe(true);
      expect(rewritten.revision).toBe(2);
      expect((await client.get(key, { durability })).valueAsString()).toBe('some-value-2');
    });

    it('deletes through the entry handle', async () => {
      const client = makeClient(transport, shape);
      const written = await client.set(randomKey(), 'some-value', { expiry: 10_000, durability });

      const deleted = await written.delete();
      expect(deleted.success).toBe(true);
    });

    it('reports whether a key holds a value', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      await client.set(key, 'some-value', { expiry: 10_000, durability });

      const exists = await client.exists(key, { durability });
      expect(exists.success).toBe(true);
      expect(exists.revision).toBe(0);
    });

    it('refuses a conditional write against a tombstone, and reports its revision', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      await client.set(key, 'some-value', { expiry: 10_000, durability });
      await client.delete(key, { durability });

      const rejected = await client.set(key, 'some-value-2', {
        expiry: 10_000,
        mode: 'ifExists',
        durability,
      });
      expect(rejected.success).toBe(false);
      expect(rejected.revision).toBe(1);

      expect((await client.get(key, { durability })).value).toBeNull();
    });

    it('writes only when the current value matches', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      await client.set(key, 'some-value', { expiry: 10_000, durability });

      const applied = await client.compareValueAndSet(key, 'some-new-value', 'some-value', {
        expiry: 10_000,
        durability,
      });
      expect(applied.success).toBe(true);
      expect(applied.revision).toBe(1);
      expect((await client.get(key, { durability })).valueAsString()).toBe('some-new-value');
    });

    it('leaves the value untouched when the comparison fails', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      await client.set(key, 'some-value', { expiry: 10_000, durability });

      const rejected = await client.compareValueAndSet(key, 'some-new-value', 'other-value', {
        expiry: 10_000,
        durability,
      });
      expect(rejected.success).toBe(false);
      expect(rejected.revision).toBe(0);
      expect((await client.get(key, { durability })).valueAsString()).toBe('some-value');
    });

    it('writes only when the current revision matches', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      await client.set(key, 'some-value', { expiry: 10_000, durability });
      await client.set(key, 'some-new-value', { expiry: 10_000, durability });

      const applied = await client.compareRevisionAndSet(key, 'some-new-new-value', 1, {
        expiry: 10_000,
        durability,
      });
      expect(applied.success).toBe(true);
      expect(applied.revision).toBe(2);
      expect((await client.get(key, { durability })).valueAsString()).toBe('some-new-new-value');
    });

    it('leaves the value untouched when the revision comparison fails', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      await client.set(key, 'some-value', { expiry: 10_000, durability });
      await client.set(key, 'some-new-value', { expiry: 10_000, durability });

      const rejected = await client.compareRevisionAndSet(key, 'some-new-new-value', 10, {
        expiry: 10_000,
        durability,
      });
      expect(rejected.success).toBe(false);
      expect(rejected.revision).toBe(1);
      expect((await client.get(key, { durability })).valueAsString()).toBe('some-new-value');
    });

    it('reads one archived revision of a key', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      await client.set(key, 'some-value', { mode: 'always', durability });
      await client.set(key, 'some-value-2', { mode: 'always', durability });

      const latest = await client.get(key, { durability });
      expect(latest.revision).toBe(1);
      expect(latest.valueAsString()).toBe('some-value-2');

      const first = await client.getRevision(key, 0, { durability });
      expect(first.success).toBe(true);
      expect(first.revision).toBe(0);
      expect(first.valueAsString()).toBe('some-value');

      await client.set(key, 'some-value-3', { mode: 'always', durability });

      const second = await client.getRevision(key, 1, { durability });
      expect(second.success).toBe(true);
      expect(second.revision).toBe(1);
      expect(second.valueAsString()).toBe('some-value-2');
    });

    it('reads every key of one bucket', async () => {
      const client = makeClient(transport, shape);
      const prefix = randomKey('bucket');

      for (let i = 0; i < 3; i++) {
        const written = await client.set(`${prefix}/${randomKey()}`, 'some-value', {
          expiry: 10_000,
          durability,
        });
        expect(written.success).toBe(true);
      }

      await sleep(1_000);

      const items = await client.getByBucket(prefix, { durability });
      expect(items).toHaveLength(3);
      for (const item of items) {
        expect(item.success).toBe(true);
        expect(item.key.startsWith(prefix)).toBe(true);
      }
    });

    it('reads an empty bucket as no rows rather than as an error', async () => {
      const client = makeClient(transport, shape);
      expect(await client.getByBucket(randomKey('empty'), { durability })).toHaveLength(0);
    });

    it('writes several keys in one request', async () => {
      const client = makeClient(transport, shape);
      const first = randomKey();
      const second = randomKey();

      const results = await client.setMany([
        { key: first, value: 'some-value 1', expiry: 10_000, durability },
        { key: second, value: 'some-value 2', expiry: 10_000, durability },
      ]);

      expect(results).toHaveLength(2);
      expect(results.every((result) => result.success)).toBe(true);
      expect(results.every((result) => result.revision === 0)).toBe(true);
    });
  },
);

describe('KahunaError', () => {
  it('names the family of the code it carries', async () => {
    const client = makeClient('grpc', 'single');

    await expect(client.set('', 'v', { expiry: 1_000 })).rejects.toMatchObject({
      name: 'KahunaError',
      domain: 'keyValue',
      code: 'invalidInput',
    });
  });

  it('is recognisable with instanceof', async () => {
    const client = makeClient('rest', 'single');
    await expect(client.set('', 'v', { expiry: 1_000 })).rejects.toBeInstanceOf(KahunaError);
  });
});
