import { afterAll, describe, expect, it } from 'vitest';

import { compareHlc, hlc, watchKey, type KahunaClient } from '../src/index.js';
import {
  closeClients,
  combinations,
  makeClient,
  randomKey,
  type ClientShape,
  type TransportKind,
} from './support/cluster.js';

afterAll(closeClients);

/** A data partition of the cluster, read off the watch of one key. */
async function somePartition(client: KahunaClient): Promise<number> {
  await using watch = await client.watchKeyValues({ scope: watchKey(randomKey('hold-probe')) });
  return watch.partitions[0]!;
}

describe.each(combinations())(
  'history holds over %s, %s endpoint',
  (transport: TransportKind, shape: ClientShape) => {
    it('acquires, lists, renews and releases a hold', async () => {
      const client = makeClient(transport, shape);
      const holderId = `test-holder-${crypto.randomUUID().slice(0, 8)}`;
      const partitionId = await somePartition(client);
      const from = hlc(0, Date.now() - 1_000, 0);

      const acquired = await client.acquireHistoryHolds(holderId, [partitionId], from, 60_000);
      expect(acquired.type).toBe('set');
      expect(acquired.holds).toHaveLength(1);

      const hold = acquired.holds[0]!;
      expect(hold.holderId).toBe(holderId);
      expect(hold.partitionId).toBe(partitionId);
      expect(hold.holdId).not.toBe('');

      try {
        const listed = await client.listHistoryHolds({ holderId });
        expect(listed.type).toBe('get');
        expect(listed.holds.map((h) => h.holdId)).toContain(hold.holdId);

        const forward = hlc(0, Date.now(), 0);
        const renewed = await client.renewHistoryHolds([hold.holdId], 120_000, { timestamp: forward });
        expect(renewed.type).toBe('set');
        expect(compareHlc(renewed.holds[0]!.timestamp, hold.timestamp)).toBeGreaterThanOrEqual(0);
        expect(compareHlc(renewed.holds[0]!.leaseExpiry, hold.leaseExpiry)).toBeGreaterThanOrEqual(0);
      } finally {
        const released = await client.releaseHistoryHolds([hold.holdId]);
        expect(released.type).toBe('deleted');
      }

      const gone = await client.renewHistoryHolds([hold.holdId], 60_000);
      expect(gone.type).toBe('doesNotExist');
    });
  },
);
