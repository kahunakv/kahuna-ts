import { afterAll, describe, expect, it } from 'vitest';

import { KahunaError } from '../src/index.js';
import {
  closeClients,
  combinations,
  makeClient,
  randomKey,
  randomLockName,
  type ClientShape,
  type TransportKind,
} from './support/cluster.js';

afterAll(closeClients);

/** Ported from `Kahuna.Client.Tests/TestClientErrorHandling.cs`. */
describe.each(combinations())(
  'error handling over %s, %s endpoint',
  (transport: TransportKind, shape: ClientShape) => {
    it('refuses an empty key', async () => {
      const client = makeClient(transport, shape);
      await expect(client.set('', 'test-value')).rejects.toThrow(/Failed to set key\/value/);
    });

    it('keeps a key that holds no value apart from one that holds zero bytes', async () => {
      const client = makeClient(transport, shape);

      // Both transports preserve an absent payload: gRPC as an unset optional field,
      // REST as an explicit JSON null.
      const absentKey = randomKey();
      expect((await client.set(absentKey, null, { expiry: 10_000 })).success).toBe(true);

      const absent = await client.get(absentKey);
      expect(absent.success).toBe(true);
      expect(absent.value).toBeNull();
      expect(absent.valueAsString()).toBeNull();

      const emptyKey = randomKey();
      expect((await client.set(emptyKey, new Uint8Array(0), { expiry: 10_000 })).success).toBe(true);

      const empty = await client.get(emptyKey);
      expect(empty.success).toBe(true);
      expect(empty.value).not.toBeNull();
      expect(empty.value).toHaveLength(0);
      expect(empty.valueAsString()).toBe('');
    });

    it('reads a key that was never written as a miss rather than an error', async () => {
      const client = makeClient(transport, shape);
      const result = await client.get(randomKey('non-existent'));

      expect(result.success).toBe(false);
      expect(result.value).toBeNull();
    });

    it('reports a delete of a key that was never written as a miss', async () => {
      const client = makeClient(transport, shape);
      expect((await client.delete(randomKey('non-existent'))).success).toBe(false);
    });

    it('reports an extend of a key that was never written as a miss', async () => {
      const client = makeClient(transport, shape);
      expect((await client.extend(randomKey('non-existent'), 10_000)).success).toBe(false);
    });

    it('refuses a compare-and-set against a key that was never written', async () => {
      const client = makeClient(transport, shape);
      const result = await client.compareValueAndSet(
        randomKey('non-existent'),
        'new-value',
        'old-value',
        { expiry: 10_000 },
      );

      expect(result.success).toBe(false);
    });

    it('refuses a compare-revision-and-set against a key that was never written', async () => {
      const client = makeClient(transport, shape);
      const result = await client.compareRevisionAndSet(randomKey('non-existent'), 'new-value', 0, {
        expiry: 10_000,
      });

      expect(result.success).toBe(false);
    });

    it('reports a syntax error in a transaction script', async () => {
      const client = makeClient(transport, shape);
      await expect(client.executeScript('INVALID SYNTAX')).rejects.toThrow(/Syntax error/);
    });

    it('refuses a lock on an empty resource name', async () => {
      const client = makeClient(transport, shape);
      await expect(client.acquireLock('', { expiry: 10_000 })).rejects.toThrow(/Failed to lock/);
    });

    it('reports a lock that does not exist', async () => {
      const client = makeClient(transport, shape);
      await expect(client.getLockInfo(randomLockName())).rejects.toMatchObject({
        domain: 'lock',
        code: 'lockDoesNotExist',
      });
    });

    it('refuses a release from a holder that does not own the lock', async () => {
      const client = makeClient(transport, shape);
      const name = randomLockName();

      await using lock = await client.acquireLock(name, { expiry: 10_000 });
      expect(lock.acquired).toBe(true);

      await expect(client.releaseLock(name, 'invalid-owner')).rejects.toMatchObject({
        domain: 'lock',
        code: 'invalidOwner',
      });
    });

    it('carries a value of one megabyte', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey();

      const large = new Uint8Array(1024 * 1024);
      // Randomising a slice is enough to prove the bytes survive; randomising the
      // whole megabyte would only slow the suite down.
      crypto.getRandomValues(large.subarray(0, 65_536));

      expect((await client.set(key, large, { expiry: 10_000 })).success).toBe(true);

      const read = await client.get(key);
      expect(read.success).toBe(true);
      expect(read.value).toHaveLength(large.length);
      expect(Buffer.from(read.value!.subarray(0, 65_536))).toEqual(
        Buffer.from(large.subarray(0, 65_536)),
      );
    });

    it('raises a KahunaError rather than a bare Error', async () => {
      const client = makeClient(transport, shape);
      await expect(client.set('', 'v')).rejects.toBeInstanceOf(KahunaError);
    });
  },
);
