import { afterAll, describe, expect, it } from 'vitest';

import {
  HLC_ZERO,
  KahunaError,
  compareHlc,
  hlc,
  isHlcZero,
  watchEventSupersedes,
  watchKey,
  watchPrefix,
  watchRange,
  watchScopeMatches,
  type KahunaWatch,
  type WatchEvent,
  type WatchFrame,
} from '../src/index.js';
import {
  closeClients,
  combinations,
  makeClient,
  randomKey,
  type ClientShape,
  type TransportKind,
} from './support/cluster.js';

afterAll(closeClients);

/**
 * Progress watermarks, and therefore resume, need the commit admission window
 * (`--commit-admission-lag-ms`) on every node. Origin labels need
 * `--transaction-origin-tags`. Set these to `1` when the cluster runs with them.
 */
const COMMIT_ADMISSION = process.env['KAHUNA_TEST_COMMIT_ADMISSION'] === '1';
const ORIGIN_TAGS = process.env['KAHUNA_TEST_ORIGIN_TAGS'] === '1';

/** How long a test waits for one frame before it fails. */
const FRAME_WAIT_MS = 15_000;

/** Reads frames until one satisfies the predicate. Fails on a terminal frame or a timeout. */
async function nextFrame(
  frames: AsyncIterator<WatchFrame>,
  accept: (frame: WatchFrame) => boolean,
): Promise<WatchFrame> {
  const deadline = Date.now() + FRAME_WAIT_MS;

  while (Date.now() < deadline) {
    const next = await Promise.race([
      frames.next(),
      new Promise<'timeout'>((resolve) => setTimeout(() => resolve('timeout'), deadline - Date.now())),
    ]);
    if (next === 'timeout') break;
    if (next.done) throw new Error('The watch ended before the expected frame');
    if (accept(next.value)) return next.value;
    if (next.value.type === 'lagged' || next.value.type === 'refused' || next.value.type === 'historyUnavailable') {
      throw new Error(`The watch ended with ${next.value.type}`);
    }
  }

  throw new Error('No matching frame arrived in time');
}

async function nextEvent(
  frames: AsyncIterator<WatchFrame>,
  accept: (event: WatchEvent) => boolean = () => true,
): Promise<WatchEvent> {
  const frame = await nextFrame(frames, (f) => f.event !== null && accept(f.event));
  return frame.event!;
}

describe('watch scopes', () => {
  it('matches keys exactly, by prefix and by range', () => {
    expect(watchScopeMatches(watchKey('a/b'), 'a/b')).toBe(true);
    expect(watchScopeMatches(watchKey('a/b'), 'a/bc')).toBe(false);

    expect(watchScopeMatches(watchPrefix('a/'), 'a/b')).toBe(true);
    expect(watchScopeMatches(watchPrefix('a/'), 'b/a')).toBe(false);

    const range = watchRange('t:r/', 't:r/b', 't:r/d');
    expect(range.value).toBe('t:r');
    expect(watchScopeMatches(range, 't:r/b')).toBe(true);
    expect(watchScopeMatches(range, 't:r/c')).toBe(true);
    expect(watchScopeMatches(range, 't:r/d')).toBe(false);
    expect(watchScopeMatches(range, 't:r/x/c')).toBe(false);
  });

  it('refuses a range whose start is not below its end', () => {
    expect(() => watchRange('t:r', 'b', 'a')).toThrow(RangeError);
    expect(() => watchKey('')).toThrow(TypeError);
  });
});

describe('watch event order', () => {
  function event(revision: number, physical: number, kind: WatchEvent['kind'] = 'set'): WatchEvent {
    return {
      key: 'k',
      kind,
      value: null,
      revision,
      expires: HLC_ZERO,
      commitTimestamp: hlc(1, physical, 0),
      transactionId: HLC_ZERO,
      partitionId: 0,
      origin: null,
    };
  }

  it('orders by commit timestamp, then by revision', () => {
    expect(watchEventSupersedes(event(1, 10), null)).toBe(true);
    expect(watchEventSupersedes(event(2, 20), event(1, 10))).toBe(true);
    expect(watchEventSupersedes(event(2, 10), event(1, 10))).toBe(true);
    expect(watchEventSupersedes(event(1, 10), event(2, 20))).toBe(false);
  });

  it('drops a repeat and keeps an extend that holds the revision', () => {
    expect(watchEventSupersedes(event(3, 30), event(3, 30))).toBe(false);
    expect(watchEventSupersedes(event(3, 40, 'extended'), event(3, 30))).toBe(true);
  });
});

describe.each(combinations())(
  'key/value watches over %s, %s endpoint',
  (transport: TransportKind, shape: ClientShape) => {
    it('reports a set and a delete of the watched key', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey('watch');

      await using watch = await client.watchKeyValues({ scope: watchKey(key) });
      expect(watch.partitions.length).toBeGreaterThan(0);
      expect(watch.endpoints.length).toBeGreaterThan(0);

      const frames = watch.frames();

      const written = await client.set(key, 'first');
      const set = await nextEvent(frames);
      expect(set.key).toBe(key);
      expect(set.kind).toBe('set');
      expect(new TextDecoder().decode(set.value!)).toBe('first');
      expect(set.revision).toBe(written.revision);
      expect(isHlcZero(set.commitTimestamp)).toBe(false);
      expect(watch.partitions).toContain(set.partitionId);

      await client.delete(key);
      const deleted = await nextEvent(frames, (event) => event.kind === 'deleted');
      expect(deleted.key).toBe(key);
      expect(deleted.value).toBeNull();
    });

    it('reports only the keys under a watched prefix', async () => {
      const client = makeClient(transport, shape);
      const prefix = `${randomKey('watch-prefix')}/`;
      const outside = randomKey('watch-outside');

      await using watch = await client.watchKeyValues({ scope: watchPrefix(prefix) });
      const frames = watch.frames();

      await client.set(outside, 'ignored');
      await client.set(`${prefix}one`, 'value');

      const event = await nextEvent(frames);
      expect(event.key).toBe(`${prefix}one`);
    });

    it('drops the value from a keys-only watch', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey('watch-keys-only');

      await using watch = await client.watchKeyValues({ scope: watchKey(key), keysOnly: true });
      const frames = watch.frames();

      await client.set(key, 'hidden');
      const event = await nextEvent(frames);
      expect(event.kind).toBe('set');
      expect(event.value).toBeNull();
    });

    it('keeps an empty value apart from no value', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey('watch-empty');

      await using watch = await client.watchKeyValues({ scope: watchKey(key) });
      const frames = watch.frames();

      await client.set(key, new Uint8Array(0));
      const event = await nextEvent(frames);
      expect(event.value).not.toBeNull();
      expect(event.value!.length).toBe(0);
    });

    it('refuses a watch on ephemeral keys', async () => {
      const client = makeClient(transport, shape);

      const error = await client
        .watchKeyValues({ scope: watchKey(randomKey('watch-ephemeral')), durability: 'ephemeral' })
        .catch((e: unknown) => e);

      expect(error).toBeInstanceOf(KahunaError);
      expect((error as KahunaError).code).toBe('watchUnsupportedOnEphemeral');
    });

    it('ends the frames without a terminal frame once closed', async () => {
      const client = makeClient(transport, shape);
      const watch: KahunaWatch = await client.watchKeyValues({ scope: watchKey(randomKey('watch-close')) });

      const reading = (async () => {
        const seen: WatchFrame[] = [];
        for await (const frame of watch) seen.push(frame);
        return seen;
      })();

      await watch.close();
      const seen = await reading;
      expect(seen.filter((frame) => frame.type === 'lagged' || frame.type === 'refused')).toEqual([]);

      // Closing twice is harmless.
      await watch.close();
    });

    it('reports a transaction commit at the timestamp the commit returned', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey('watch-tx');
      const origin = 'schema-rewrite';

      await using watch = await client.watchKeyValues({ scope: watchKey(key) });
      const frames = watch.frames();

      const session = await client.beginTransaction({ locking: 'optimistic', origin });
      await session.set(key, 'from-transaction');
      const { committed, commitTimestamp } = await session.commitWithTimestamp();
      expect(committed).toBe(true);
      expect(isHlcZero(commitTimestamp)).toBe(false);

      const event = await nextEvent(frames);
      expect(event.key).toBe(key);
      expect(compareHlc(event.commitTimestamp, commitTimestamp)).toBe(0);
      expect(event.transactionId).toEqual(session.transactionId);
      expect(event.origin).toBe(ORIGIN_TAGS ? origin : null);
    });

    it.runIf(COMMIT_ADMISSION)('replays missed events from the last watermarks', async () => {
      const client = makeClient(transport, shape);
      const prefix = `${randomKey('watch-resume')}/`;

      const first = await client.watchKeyValues({ scope: watchPrefix(prefix) });
      const firstFrames = first.frames();

      await client.set(`${prefix}before`, 'seen');
      await nextEvent(firstFrames);

      // Wait until every partition of the watch has a watermark, so each one has a cursor.
      await nextFrame(
        firstFrames,
        () => first.partitions.every((partitionId) => first.watermarks.has(partitionId)),
      );
      const cursors = first.watermarks;
      await first.close();

      await client.set(`${prefix}missed`, 'replayed');

      await using second = await client.watchKeyValues({
        scope: watchPrefix(prefix),
        resumePartitions: cursors,
      });

      const replayed = await nextEvent(second.frames(), (event) => event.key === `${prefix}missed`);
      expect(new TextDecoder().decode(replayed.value!)).toBe('replayed');
    });

    it.runIf(COMMIT_ADMISSION)('reports progress watermarks that only move forward', async () => {
      const client = makeClient(transport, shape);
      const key = randomKey('watch-progress');

      await using watch = await client.watchKeyValues({ scope: watchKey(key) });
      const frames = watch.frames();

      await client.set(key, 'value');
      const progress = await nextFrame(frames, (frame) => frame.type === 'progress');
      expect(isHlcZero(progress.timestamp)).toBe(false);
      expect(compareHlc(watch.watermarks.get(progress.partitionId) ?? HLC_ZERO, progress.timestamp)).toBeGreaterThanOrEqual(0);
    });
  },
);
