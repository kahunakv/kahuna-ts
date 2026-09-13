import { afterAll, describe, expect, it } from 'vitest';

import { HLC_ZERO, compareHlc, hlc, hlcEquals } from '../src/index.js';
import {
  closeClients,
  combinations,
  makeClient,
  type ClientShape,
  type TransportKind,
} from './support/cluster.js';

afterAll(closeClients);

/**
 * Ported from `Kahuna.Client.Tests/TestSnapshotHold.cs`.
 *
 * The snapshot floor is cluster-global state, so these cases must not run beside
 * each other. The suite sets `fileParallelism: false` for that reason.
 */
const TEST_FLOOR = hlc(42, 1_704_067_200_000, 7);

describe.each(combinations())(
  'snapshot holds over %s, %s endpoint',
  (transport: TransportKind, shape: ClientShape) => {
    it('round-trips every field of the timestamp it pinned', async () => {
      const client = makeClient(transport, shape);
      const holderId = `test-holder-${crypto.randomUUID().slice(0, 8)}`;

      const hold = await client.acquireSnapshotHold(holderId, TEST_FLOOR, 60_000);
      expect(hold.type).toBe('set');
      expect(hold.holdId).not.toBe('');
      expect(hlcEquals(hold.leaseExpiry, HLC_ZERO)).toBe(false);

      try {
        const floor = await client.getSnapshotFloor();
        // All three fields must survive, so a swap of node id and counter is caught.
        expect(floor.effectiveFloor).toEqual(TEST_FLOOR);
        expect(floor.liveHolds).toBeGreaterThanOrEqual(1);
      } finally {
        expect(await client.releaseSnapshotHold(hold.holdId)).toBe('deleted');
      }
    });

    it('pushes the lease expiry out on renewal', async () => {
      const client = makeClient(transport, shape);
      const holderId = `renew-holder-${crypto.randomUUID().slice(0, 8)}`;

      const hold = await client.acquireSnapshotHold(holderId, TEST_FLOOR, 30_000);
      expect(hold.type).toBe('set');

      try {
        const renewed = await client.renewSnapshotHold(hold.holdId, 120_000);
        expect(renewed.type).toBe('set');
        expect(compareHlc(renewed.leaseExpiry, hold.leaseExpiry)).toBeGreaterThanOrEqual(0);
      } finally {
        await client.releaseSnapshotHold(hold.holdId);
      }
    });

    it('drops the floor to zero once the last hold is released', async () => {
      const client = makeClient(transport, shape);
      const holderId = `release-holder-${crypto.randomUUID().slice(0, 8)}`;

      const hold = await client.acquireSnapshotHold(holderId, TEST_FLOOR, 60_000);
      expect(hold.holdId).not.toBe('');

      const before = await client.getSnapshotFloor();
      expect(before.effectiveFloor).toEqual(TEST_FLOOR);

      expect(await client.releaseSnapshotHold(hold.holdId)).toBe('deleted');

      const after = await client.getSnapshotFloor();
      expect(after.effectiveFloor).toEqual(HLC_ZERO);
      expect(after.liveHolds).toBe(0);
    });

    it('reports the lowest of several holds as the effective floor', async () => {
      const client = makeClient(transport, shape);

      // Two timestamps with every field distinct, so a confusion of node, physical
      // time and counter is caught.
      const lower = hlc(3, 1_000_000_000, 2);
      const higher = hlc(5, 2_000_000_000, 9);

      const first = await client.acquireSnapshotHold(
        `floor-min-a-${crypto.randomUUID().slice(0, 8)}`,
        lower,
        60_000,
      );
      const second = await client.acquireSnapshotHold(
        `floor-min-b-${crypto.randomUUID().slice(0, 8)}`,
        higher,
        60_000,
      );

      try {
        const floor = await client.getSnapshotFloor();
        expect(floor.effectiveFloor).toEqual(lower);
        expect(floor.liveHolds).toBeGreaterThanOrEqual(2);

        await client.releaseSnapshotHold(first.holdId);

        const raised = await client.getSnapshotFloor();
        expect(raised.effectiveFloor).toEqual(higher);
      } finally {
        await client.releaseSnapshotHold(second.holdId);
      }
    });
  },
);
