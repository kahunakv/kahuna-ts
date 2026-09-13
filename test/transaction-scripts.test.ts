import { afterAll, describe, expect, it } from 'vitest';

import { bytesToText } from '../src/transport/codec.js';
import {
  closeClients,
  combinations,
  makeClient,
  randomKey,
  type ClientShape,
  type TransportKind,
} from './support/cluster.js';

afterAll(closeClients);

/** Ported from `Kahuna.Client.Tests/TestKeyValueTransactions.cs`. */
describe.each(combinations())(
  'transaction scripts over %s, %s endpoint',
  (transport: TransportKind, shape: ClientShape) => {
    it('writes a key', async () => {
      const client = makeClient(transport, shape);
      const result = await client.executeScript(`SET \`${randomKey()}\` 'some value'`);

      expect(result.type).toBe('set');
      expect(result.values[0]?.revision).toBe(0);
    });

    it('substitutes a named parameter', async () => {
      const client = makeClient(transport, shape);
      const result = await client.executeScript(`SET \`${randomKey()}\` @value`, {
        parameters: [{ key: '@value', value: 'some value' }],
      });

      expect(result.type).toBe('set');
      expect(result.values[0]?.revision).toBe(0);
    });

    it('reads a key back', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      expect((await client.executeScript(`SET \`${key}\` 'some value'`)).type).toBe('set');

      const read = await client.executeScript(`GET \`${key}\``);
      expect(read.type).toBe('get');
      expect(read.values[0]?.revision).toBe(0);
      expect(bytesToText(read.values[0]?.value ?? null)).toBe('some value');
    });

    it('extends a key', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      await client.executeScript(`SET \`${key}\` 'some value'`);

      const extended = await client.executeScript(`EXTEND \`${key}\` 1000`);
      expect(extended.type).toBe('extended');
      expect(extended.values[0]?.revision).toBe(0);
    });

    it('reports whether a key exists', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      await client.executeScript(`SET \`${key}\` 'some value'`);

      const exists = await client.executeScript(`EXISTS \`${key}\``);
      expect(exists.type).toBe('exists');
      expect(exists.values[0]?.revision).toBe(0);
    });

    it('records a delete as a revision of its own', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      await client.executeScript(`SET \`${key}\` 'some value'`);

      const deleted = await client.executeScript(`DELETE \`${key}\``);
      expect(deleted.type).toBe('deleted');
      // The tombstone is the live revision plus one, and both wires carry the
      // per-value revision.
      expect(deleted.values[0]?.revision).toBe(1);
    });

    it('runs a multi-statement transaction', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      const script = client.loadScript(`
BEGIN (locking="optimistic")
 SET @key @value
 COMMIT
END
`);

      const result = await script.run({
        parameters: [
          { key: '@key', value: key },
          { key: '@value', value: 'script-value' },
        ],
      });

      expect(result.type).toBe('set');
      expect((await client.get(key)).valueAsString()).toBe('script-value');
    });

    it('keys the parsed-script cache by the digest of the exact bytes', async () => {
      const client = makeClient(transport, shape);
      const first = client.loadScript(`SET \`${randomKey()}\` 'v'`);
      const second = client.loadScript(first.bytes);

      // The digest is BLAKE3 over the bytes, so identical text gives one cache key
      // and a change of a single character gives another.
      expect(second.hash).toBe(first.hash);
      expect(client.loadScript(`SET \`k\` 'w'`).hash).not.toBe(first.hash);
      expect(first.hash).toMatch(/^[0-9a-f]{64}$/);
    });

    it('runs a loaded script twice, reusing the server-side parse', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();
      const script = client.loadScript(`SET @key @value`);

      for (const value of ['one', 'two']) {
        const result = await script.run({
          parameters: [
            { key: '@key', value: key },
            { key: '@value', value },
          ],
        });
        expect(result.type).toBe('set');
      }

      expect((await client.get(key)).valueAsString()).toBe('two');
    });

    it('reports a syntax error rather than a retry', async () => {
      const client = makeClient(transport, shape);
      await expect(client.executeScript('INVALID SYNTAX')).rejects.toMatchObject({
        domain: 'keyValue',
      });
    });
  },
);
