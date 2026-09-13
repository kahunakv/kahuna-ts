import { afterAll, describe, expect, it } from 'vitest';

import { KahunaError } from '../src/index.js';
import {
  closeClients,
  combinations,
  makeClient,
  type ClientShape,
  type TransportKind,
} from './support/cluster.js';

afterAll(closeClients);

function randomSequenceName(): string {
  return `seq-${crypto.randomUUID().replaceAll('-', '').slice(0, 12)}`;
}

/**
 * The .NET suite has no sequence test file, so these cover the sequencer against
 * the same contract the .NET client implements.
 */
describe.each(combinations())(
  'sequences over %s, %s endpoint',
  (transport: TransportKind, shape: ClientShape) => {
    it('creates a sequence and reads it back', async () => {
      const client = makeClient(transport, shape);
      const name = randomSequenceName();

      const created = await client.createSequence(name, { initialValue: 10, increment: 2 });
      expect(created.name).toBe(name);
      expect(created.initialValue).toBe(10);
      expect(created.increment).toBe(2);
      expect(created.durability).toBe('persistent');

      const read = await client.getSequence(name);
      expect(read).not.toBeNull();
      expect(read!.name).toBe(name);

      expect(await client.deleteSequence(name)).toBe(true);
    });

    it('reads a sequence that does not exist as null', async () => {
      const client = makeClient(transport, shape);
      expect(await client.getSequence(randomSequenceName())).toBeNull();
    });

    it('reports a delete of a sequence that does not exist as false', async () => {
      const client = makeClient(transport, shape);
      expect(await client.deleteSequence(randomSequenceName())).toBe(false);
    });

    it('hands out values in increments, never repeating one', async () => {
      const client = makeClient(transport, shape);
      const name = randomSequenceName();

      await client.createSequence(name, { initialValue: 0, increment: 5 });

      try {
        const first = await client.nextSequenceValue(name);
        const second = await client.nextSequenceValue(name);
        const third = await client.nextSequenceValue(name);

        expect(second - first).toBe(5);
        expect(third - second).toBe(5);
      } finally {
        await client.deleteSequence(name);
      }
    });

    it('reserves a block of values for one caller alone', async () => {
      const client = makeClient(transport, shape);
      const name = randomSequenceName();

      await client.createSequence(name, { initialValue: 0, increment: 1 });

      try {
        const range = await client.reserveSequenceRange(name, 10);
        expect(range.name).toBe(name);
        expect(range.count).toBe(10);
        expect(range.end).toBeGreaterThan(range.start);

        // The next value lands past the whole reserved block, so no value is ever
        // handed to two callers.
        const next = await client.nextSequenceValue(name);
        expect(next).toBeGreaterThan(range.end - 1);
      } finally {
        await client.deleteSequence(name);
      }
    });

    it('consumes one value for a repeated request that carries the same key', async () => {
      const client = makeClient(transport, shape);
      const name = randomSequenceName();
      const idempotencyKey = crypto.randomUUID();

      await client.createSequence(name, { initialValue: 0, increment: 1 });

      try {
        const first = await client.nextSequenceValue(name, { idempotencyKey });
        const repeated = await client.nextSequenceValue(name, { idempotencyKey });

        expect(repeated).toBe(first);
      } finally {
        await client.deleteSequence(name);
      }
    });

    it('refuses to create the same sequence twice', async () => {
      const client = makeClient(transport, shape);
      const name = randomSequenceName();

      await client.createSequence(name);

      try {
        await expect(client.createSequence(name)).rejects.toMatchObject({
          domain: 'sequence',
          code: 'alreadyExists',
        });
      } finally {
        await client.deleteSequence(name);
      }
    });

    it('refuses to hand out a value past the sequence maximum', async () => {
      const client = makeClient(transport, shape);
      const name = randomSequenceName();

      await client.createSequence(name, { initialValue: 0, increment: 1, maxValue: 2 });

      try {
        await client.nextSequenceValue(name);
        await client.nextSequenceValue(name);

        await expect(client.nextSequenceValue(name)).rejects.toBeInstanceOf(KahunaError);
      } finally {
        await client.deleteSequence(name);
      }
    });

    it('creates a sequence with its own block size', async () => {
      const client = makeClient(transport, shape);
      const name = randomSequenceName();

      const created = await client.createSequence(name, { blockSize: 1 });

      try {
        expect(created.blockSize).toBe(1);
        expect(created.incarnation).toBe(0);
      } finally {
        await client.deleteSequence(name);
      }
    });

    it('reads a sequence created without a block size as null', async () => {
      const client = makeClient(transport, shape);
      const name = randomSequenceName();

      const created = await client.createSequence(name);

      try {
        expect(created.blockSize).toBeNull();
      } finally {
        await client.deleteSequence(name);
      }
    });

    it('restarts a sequence at a new current value', async () => {
      const client = makeClient(transport, shape);
      const name = randomSequenceName();

      await client.createSequence(name, { initialValue: 0, increment: 1 });

      try {
        await client.nextSequenceValue(name);

        const updated = await client.updateSequence(name, { currentValue: 5000, increment: 10 });
        expect(updated.currentValue).toBe(5000);
        expect(updated.increment).toBe(10);
        expect(updated.incarnation).toBe(1);

        expect(await client.nextSequenceValue(name)).toBe(5010);
      } finally {
        await client.deleteSequence(name);
      }
    });

    it('sets and removes the maximum and the block size', async () => {
      const client = makeClient(transport, shape);
      const name = randomSequenceName();

      await client.createSequence(name, { initialValue: 0, increment: 1 });

      try {
        const set = await client.updateSequence(name, { maxValue: 1000, blockSize: 4 });
        expect(set.maxValue).toBe(1000);
        expect(set.blockSize).toBe(4);
        expect(set.initialValue).toBe(0);

        const removed = await client.updateSequence(name, {
          removeMaxValue: true,
          removeBlockSize: true,
        });
        expect(removed.maxValue).toBeNull();
        expect(removed.blockSize).toBeNull();
        expect(removed.incarnation).toBe(2);
      } finally {
        await client.deleteSequence(name);
      }
    });

    it('refuses an update that changes nothing', async () => {
      const client = makeClient(transport, shape);
      const name = randomSequenceName();

      await client.createSequence(name);

      try {
        await expect(client.updateSequence(name, {})).rejects.toMatchObject({
          domain: 'sequence',
          code: 'invalidInput',
        });
      } finally {
        await client.deleteSequence(name);
      }
    });

    it('refuses to update a sequence that does not exist', async () => {
      const client = makeClient(transport, shape);

      await expect(
        client.updateSequence(randomSequenceName(), { currentValue: 10 }),
      ).rejects.toMatchObject({ domain: 'sequence', code: 'notFound' });
    });
  },
);
