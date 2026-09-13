import { afterAll, describe, expect, it } from 'vitest';

import type { Durability } from '../src/index.js';
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

/** Ported from `Kahuna.Client.Tests/TestAdvancedKeyValueOperations.cs`. */
describe.each(durableCombinations())(
  'batches and scans over %s, %s endpoint, %s durability',
  (transport: TransportKind, shape: ClientShape, durability: Durability) => {
    it('writes ten keys in one request and reads each one back', async () => {
      const client = makeClient(transport, shape);
      const prefix = randomKey('batch');

      const items = Array.from({ length: 10 }, (_, index) => ({
        key: `${prefix}-${index}`,
        value: `value-${index}`,
        expiry: 10_000,
        durability,
      }));

      const written = await client.setMany(items);
      expect(written).toHaveLength(items.length);
      expect(written.every((result) => result.success)).toBe(true);

      for (let index = 0; index < items.length; index++) {
        const read = await client.get(`${prefix}-${index}`, { durability });
        expect(read.success).toBe(true);
        expect(read.valueAsString()).toBe(`value-${index}`);
      }
    });

    it('reads several keys in one request, and reports the misses among them', async () => {
      const client = makeClient(transport, shape);
      const prefix = randomKey('many');

      await client.setMany([
        { key: `${prefix}-a`, value: 'va', expiry: 10_000, durability },
        { key: `${prefix}-b`, value: 'vb', expiry: 10_000, durability },
      ]);

      const read = await client.getMany([
        { key: `${prefix}-a`, durability },
        { key: `${prefix}-b`, durability },
        { key: `${prefix}-missing`, durability },
      ]);

      // A batch spans several partitions, so the server answers in whatever order
      // those partitions reply. Match a row by its key, never by its position.
      const byKey = new Map(read.map((item) => [item.key, item]));

      expect(read).toHaveLength(3);
      expect(byKey.get(`${prefix}-a`)?.valueAsString()).toBe('va');
      expect(byKey.get(`${prefix}-b`)?.valueAsString()).toBe('vb');
      expect(byKey.get(`${prefix}-missing`)?.success).toBe(false);
    });

    it('reports for several keys whether each holds a value', async () => {
      const client = makeClient(transport, shape);
      const prefix = randomKey('exists');

      await client.set(`${prefix}-a`, 'va', { expiry: 10_000, durability });

      const read = await client.existsMany([
        { key: `${prefix}-a`, durability },
        { key: `${prefix}-missing`, durability },
      ]);

      const byKey = new Map(read.map((item) => [item.key, item.success]));
      expect(byKey.get(`${prefix}-a`)).toBe(true);
      expect(byKey.get(`${prefix}-missing`)).toBe(false);
    });

    it('reads only the keys of the bucket it was asked for', async () => {
      const client = makeClient(transport, shape);
      const bucket = randomKey('bucket');
      const other = randomKey('other-bucket');

      for (let index = 0; index < 10; index++) {
        await client.set(`${bucket}/key-${index}`, `value-${index}`, { expiry: 10_000, durability });
      }
      for (let index = 0; index < 5; index++) {
        await client.set(`${other}/key-${index}`, `other-${index}`, { expiry: 10_000, durability });
      }

      await sleep(500);

      const items = await client.getByBucket(bucket, { durability });
      expect(items).toHaveLength(10);
      expect(items.every((item) => item.key.startsWith(`${bucket}/`))).toBe(true);

      for (let index = 0; index < 10; index++) {
        const match = items.find((item) => item.key === `${bucket}/key-${index}`);
        expect(match?.valueAsString()).toBe(`value-${index}`);
      }
    });

    it('reads one page of an ordered range', async () => {
      const client = makeClient(transport, shape);
      const prefix = randomKey('range');

      for (let index = 0; index < 5; index++) {
        await client.set(`${prefix}/key-${index}`, `value-${index}`, { expiry: 10_000, durability });
      }

      await sleep(500);

      const page = await client.getByRange({ prefix }, { limit: 100, durability });
      expect(page).toHaveLength(5);
      expect(page.map((item) => item.key)).toEqual([...page.map((item) => item.key)].sort());
    });

    it('walks an ordered range across several pages', async () => {
      const client = makeClient(transport, shape);
      const prefix = randomKey('scan');

      for (let index = 0; index < 5; index++) {
        await client.set(`${prefix}/key-${index}`, `value-${index}`, { expiry: 10_000, durability });
      }

      await sleep(500);

      const keys: string[] = [];
      // A page size below the row count forces the paging path rather than one read.
      for await (const item of client.scanByRange({ prefix }, { limit: 2, durability })) {
        keys.push(item.key);
      }

      expect(keys).toHaveLength(5);
      expect(new Set(keys).size).toBe(5);
    });

    it('keeps an archived revision readable after later writes', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      expect((await client.set(key, 'value-1', { expiry: 10_000, durability })).revision).toBe(0);
      expect((await client.set(key, 'value-2', { expiry: 10_000, durability })).revision).toBe(1);
      expect((await client.set(key, 'value-3', { expiry: 10_000, durability })).revision).toBe(2);

      const latest = await client.get(key, { durability });
      expect(latest.valueAsString()).toBe('value-3');
      expect(latest.revision).toBe(2);

      const archived = await client.getRevision(key, 1, { durability });
      expect(archived.success).toBe(true);
      expect(archived.revision).toBe(1);
      expect(archived.valueAsString()).toBe('value-2');
    });

    it('writes no history when the caller asks for none', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      await client.setNoRevision(key, 'value-1', { expiry: 10_000, durability });
      const second = await client.setNoRevision(key, 'value-2', { expiry: 10_000, durability });

      // The revision counter still advances; only the archived entry is skipped.
      expect(second.revision).toBe(1);
      expect((await client.get(key, { durability })).valueAsString()).toBe('value-2');
    });
  },
);
