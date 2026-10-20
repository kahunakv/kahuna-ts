import { isTransientKeyValueCode, type KeyValueResponseCode, type WatchLagReason } from './enums.js';
import { KahunaError, OperationAbortedError, isAbortError, throwIfAborted } from './errors.js';
import { HLC_ZERO, compareHlc, type HlcTimestamp } from './hlc.js';
import type { RoutingEndpointPolicy } from './routing/endpoint-policy.js';
import { delay } from './transport/retry.js';
import type { Transport, WatchOpening, WatchStream } from './transport/transport.js';
import type { WatchFrame, WatchRequest, WatchRoute, WatchScope } from './types.js';

// ── Scopes ───────────────────────────────────────────────────────────────────

/** A watch on one exact key. */
export function watchKey(key: string): WatchScope {
  if (!key) throw new TypeError('A watched key cannot be empty');
  return Object.freeze({ kind: 'key', value: key, startKey: null, endKey: null });
}

/** A watch on every key that starts with a prefix. */
export function watchPrefix(prefix: string): WatchScope {
  if (!prefix) throw new TypeError('A watched prefix cannot be empty');
  return Object.freeze({ kind: 'prefix', value: prefix, startKey: null, endKey: null });
}

/**
 * A watch on every key of a key space in `[startKey, endKey)`.
 *
 * The bounds are full keys, compared ordinally, as a range scan compares them. A
 * null bound is unbounded.
 */
export function watchRange(
  keySpace: string,
  startKey: string | null = null,
  endKey: string | null = null,
): WatchScope {
  if (!keySpace) throw new TypeError('A watched key space cannot be empty');

  if (startKey !== null && endKey !== null && compareOrdinal(startKey, endKey) >= 0) {
    throw new RangeError('The start key must be below the end key');
  }

  // "t:r" and "t:r/" name the same key space, as they do for a range scan.
  const value = keySpace.endsWith('/') ? keySpace.slice(0, -1) : keySpace;
  return Object.freeze({ kind: 'range', value, startKey, endKey });
}

/** True when the key is inside the scope. */
export function watchScopeMatches(scope: WatchScope, key: string): boolean {
  switch (scope.kind) {
    case 'key':
      return key === scope.value;

    case 'prefix':
      return key.startsWith(scope.value);

    default: {
      // A key belongs to a key space when the text before its last '/' is the key space.
      const separator = key.lastIndexOf('/');
      const keySpace = separator < 0 ? key : key.slice(0, separator);
      if (keySpace !== scope.value) return false;
      if (scope.startKey !== null && compareOrdinal(key, scope.startKey) < 0) return false;
      return scope.endKey === null || compareOrdinal(key, scope.endKey) < 0;
    }
  }
}

/** Compares UTF-16 code units, as .NET's ordinal comparison does. */
function compareOrdinal(left: string, right: string): number {
  if (left === right) return 0;
  return left < right ? -1 : 1;
}

// ── Frames ───────────────────────────────────────────────────────────────────

/** True when the frame ends the stream. */
export function isTerminalWatchFrame(frame: WatchFrame): boolean {
  return frame.type === 'lagged' || frame.type === 'historyUnavailable' || frame.type === 'refused';
}

/** @internal */
export function laggedWatchFrame(
  partitionId: number,
  reason: WatchLagReason,
  lastWatermark: HlcTimestamp,
): WatchFrame {
  return {
    type: 'lagged',
    partitionId,
    event: null,
    timestamp: lastWatermark,
    transactionId: HLC_ZERO,
    lagReason: reason,
    refusal: 'get',
    partitions: [],
    routes: [],
  };
}

/** @internal */
export function refusedWatchFrame(
  partitionId: number,
  refusal: KeyValueResponseCode,
  routes: WatchRoute[] = [],
): WatchFrame {
  return {
    type: 'refused',
    partitionId,
    event: null,
    timestamp: HLC_ZERO,
    transactionId: HLC_ZERO,
    lagReason: 'none',
    refusal,
    partitions: [],
    routes,
  };
}

/** @internal A refusal with no stream. */
export function refusedWatchOpening(
  type: KeyValueResponseCode,
  routes: WatchRoute[] = [],
): WatchOpening {
  return { type, stream: null, partitions: [], routes };
}

/** @internal The opening that the first frame of a watch announces. */
export function watchOpeningFromAnswer(answer: WatchFrame, stream: WatchStream): WatchOpening {
  if (answer.type === 'opened') {
    return { type: 'get', stream, partitions: answer.partitions, routes: [] };
  }
  return refusedWatchOpening(answer.type === 'refused' ? answer.refusal : 'errored', answer.routes);
}

// ── The watch ────────────────────────────────────────────────────────────────

/** Consecutive resumes of one node stream with no frame in between before the watch gives up on it. */
const MAX_CONSECUTIVE_RESUMES = 8;

/**
 * Frames buffered between the node streams and the reader. A full buffer holds the
 * node streams back, and the serving node then ends the watch with an overflow once
 * its own buffer is full.
 */
const MERGE_CAPACITY = 1024;

/** The ending of a watch that `close` closed. The reader never sees it. */
const CLOSED_FRAME: WatchFrame = laggedWatchFrame(-1, 'none', HLC_ZERO);

/** @internal One node stream of a watch: the node, the stream, and the partitions it serves. */
export interface WatchPart {
  readonly url: string;
  readonly stream: WatchStream;
  readonly partitions: readonly number[];
}

/**
 * @internal Reopens one node stream of a watch from the cursors. Returns the new part, or
 * the frame that ends the watch when the stream cannot be reopened.
 */
export type WatchResumer = (
  part: WatchPart,
  cursors: ReadonlyMap<number, HlcTimestamp>,
  signal: AbortSignal,
) => Promise<{ part: WatchPart | null; failure: WatchFrame | null }>;

/**
 * An open watch: the frames of every committed mutation in its scope, from the
 * nodes that serve it. Iterate it to read the frames, and close it to close the
 * watch on every node; `await using` does the same through `Symbol.asyncDispose`.
 *
 * **Registration.** The watch is registered on every partition of its scope before
 * `watchKeyValues` returns it. A write committed after that produces an event, once
 * the serving replica applied it.
 *
 * **Delivery.** Delivery is at-least-once. For one key, the first delivery of each
 * revision arrives in revision order, but a repeat of an older revision can arrive
 * later. A consumer drops an event whose revision is at or below the last revision
 * it holds for that key. Across keys no order is promised.
 *
 * **Endings.** The stream ends with one terminal frame and nothing after it:
 * `lagged` when continuity is lost, `historyUnavailable` when a resume cannot be
 * served, or `refused`. Events between the last delivered one and the end can be
 * missing. The consumer opens a new watch from its last watermarks to have them
 * replayed. With no watermark, or after `historyUnavailable`, it treats everything
 * the watch covered as possibly changed.
 *
 * **Fan-out.** When no single node hosts every partition of the scope, the watch
 * runs on several nodes. A terminal frame from any of them ends the whole watch.
 *
 * **Resume.** The watch keeps the last progress watermark of each partition. With
 * automatic resume on, a node stream that ends with `lagged` (an overflow, a dropped
 * connection, a shutdown, a replaced partition state, a failed apply) is reopened
 * from those watermarks, and the node replays what was missed. The consumer sees no
 * `lagged` frame, but can see events again. A stream is resumed only when every one
 * of its partitions has a watermark. A `historyUnavailable` frame, a refusal, a
 * range-map change and a watermark violation are never hidden: they end the watch.
 */
export class KahunaWatch implements AsyncIterable<WatchFrame>, AsyncDisposable {
  readonly #queue = new FrameQueue(MERGE_CAPACITY);

  readonly #stop = new AbortController();

  readonly #pumps: Promise<void>[];

  readonly #resumer: WatchResumer | null;

  readonly #watermarks = new Map<number, HlcTimestamp>();

  #terminal: WatchFrame | null = null;

  /** The keys the watch covers. */
  readonly scope: WatchScope;

  /** The data partitions the watch is registered on, across every serving node, sorted. */
  readonly partitions: readonly number[];

  /** The nodes that serve the watch. */
  readonly endpoints: readonly string[];

  /** @internal Built by `KahunaClient.watchKeyValues`. */
  constructor(
    scope: WatchScope,
    parts: readonly WatchPart[],
    partitions: readonly number[],
    endpoints: readonly string[],
    resumer: WatchResumer | null = null,
  ) {
    this.scope = scope;
    this.partitions = partitions;
    this.endpoints = endpoints;
    this.#resumer = resumer;
    this.#pumps = parts.map((part) => this.#pump(part));
  }

  /**
   * The last progress watermark received for each partition: no event at or below
   * it arrives later on that partition. It is the resume cursor of the partition.
   * Empty while the nodes send no progress, which is when the commit admission window
   * is off.
   */
  get watermarks(): ReadonlyMap<number, HlcTimestamp> {
    return new Map(this.#watermarks);
  }

  /**
   * Reads the frames until the terminal frame, which comes last, or until the signal
   * aborts. Only one reader may read a watch. A watch that `close` closed ends
   * without a frame.
   */
  async *frames(signal?: AbortSignal): AsyncGenerator<WatchFrame> {
    for (;;) {
      const frame = await this.#queue.pull(signal);
      if (frame === null) break;
      yield frame;
    }

    const end = this.#terminal;
    if (end !== null && end !== CLOSED_FRAME) yield end;
  }

  [Symbol.asyncIterator](): AsyncIterator<WatchFrame> {
    return this.frames();
  }

  /** Closes the watch on every node. Idempotent. */
  async close(): Promise<void> {
    this.#end(CLOSED_FRAME);
    await Promise.allSettled(this.#pumps);
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  async #pump(part: WatchPart): Promise<void> {
    let current = part;
    let consecutiveResumes = 0;

    try {
      for (;;) {
        let end: WatchFrame | null = null;

        for await (const frame of current.stream.frames(this.#stop.signal)) {
          if (isTerminalWatchFrame(frame)) {
            end = frame;
            break;
          }

          consecutiveResumes = 0;

          if (frame.type === 'progress') {
            const known = this.#watermarks.get(frame.partitionId);
            if (known === undefined || compareHlc(frame.timestamp, known) > 0) {
              this.#watermarks.set(frame.partitionId, frame.timestamp);
            }
          }

          if (!(await this.#queue.push(frame))) return;
        }

        // A stream that ended with no frame was closed by this watch.
        if (end === null) return;

        const cursors = this.#cursors(current.partitions);
        if (
          this.#resumer === null ||
          consecutiveResumes >= MAX_CONSECUTIVE_RESUMES ||
          !isResumable(end) ||
          cursors === null
        ) {
          this.#end(end);
          return;
        }

        await current.stream.close();

        const resumed = await this.#resumer(current, cursors, this.#stop.signal);
        if (resumed.part === null) {
          this.#end(resumed.failure ?? end);
          return;
        }

        current = resumed.part;
        consecutiveResumes++;
      }
    } catch (error) {
      if (!isAbortError(error)) this.#end(laggedWatchFrame(-1, 'disconnected', HLC_ZERO));
    } finally {
      await current.stream.close().catch(() => undefined);
    }
  }

  /**
   * Every partition of the stream needs a cursor. A partition with none could have
   * events the consumer never received, below any timestamp the client could pick.
   */
  #cursors(partitions: readonly number[]): Map<number, HlcTimestamp> | null {
    if (partitions.length === 0) return null;

    const cursors = new Map<number, HlcTimestamp>();
    for (const partitionId of partitions) {
      const cursor = this.#watermarks.get(partitionId);
      if (cursor === undefined) return null;
      cursors.set(partitionId, cursor);
    }
    return cursors;
  }

  /**
   * Sets the frame that ends the watch (the first one wins), stops every node stream
   * and lets the reader drain what is already buffered before it gets that frame.
   */
  #end(frame: WatchFrame): void {
    if (this.#terminal !== null) return;
    this.#terminal = frame;
    this.#stop.abort();
    this.#queue.close();
  }
}

/**
 * A continuity loss a replay closes. A range-map change moved the scope's
 * partitions, and a watermark violation means a watermark cannot serve as a cursor:
 * both end the watch.
 */
function isResumable(end: WatchFrame): boolean {
  if (end.type !== 'lagged') return false;
  switch (end.lagReason) {
    case 'overflow':
    case 'disconnected':
    case 'shutdown':
    case 'partitionStateReplaced':
    case 'applyDiscontinuity':
      return true;
    default:
      return false;
  }
}

/**
 * A bounded queue with one reader and many writers.
 *
 * A writer waits while the queue is full. Closing the queue releases every waiting
 * writer and lets the reader drain what is already in the queue.
 */
class FrameQueue {
  readonly #items: WatchFrame[] = [];

  readonly #writers: (() => void)[] = [];

  #reader: (() => void) | null = null;

  #closed = false;

  constructor(private readonly capacity: number) {}

  /** Adds a frame. Resolves to false when the queue is closed and took nothing. */
  async push(frame: WatchFrame): Promise<boolean> {
    while (!this.#closed && this.#items.length >= this.capacity) {
      await new Promise<void>((resolve) => this.#writers.push(resolve));
    }
    if (this.#closed) return false;

    this.#items.push(frame);
    this.#wakeReader();
    return true;
  }

  /** The next frame, or null once the queue is closed and drained. */
  async pull(signal: AbortSignal | undefined): Promise<WatchFrame | null> {
    for (;;) {
      throwIfAborted(signal);

      const frame = this.#items.shift();
      if (frame !== undefined) {
        this.#writers.shift()?.();
        return frame;
      }
      if (this.#closed) return null;

      await this.#waitForWriter(signal);
    }
  }

  close(): void {
    this.#closed = true;
    for (const wake of this.#writers.splice(0)) wake();
    this.#wakeReader();
  }

  #wakeReader(): void {
    const reader = this.#reader;
    this.#reader = null;
    reader?.();
  }

  #waitForWriter(signal: AbortSignal | undefined): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const onAbort = (): void => {
        this.#reader = null;
        reject(new OperationAbortedError('Operation aborted', { cause: signal?.reason }));
      };

      this.#reader = () => {
        signal?.removeEventListener('abort', onAbort);
        resolve();
      };

      signal?.addEventListener('abort', onAbort, { once: true });
    });
  }
}

// ── Opening ──────────────────────────────────────────────────────────────────

/** A watch whose opening keeps racing a leader change or a range-map change gives up after this many tries. */
const WATCH_OPEN_ATTEMPTS = 8;

/**
 * @internal Opens watches for a client: on one node when it hosts the whole scope,
 * else on the replicas that node names.
 */
export class WatchOpener {
  constructor(
    private readonly transport: Transport,
    private readonly endpoints: RoutingEndpointPolicy,
    private readonly nextEndpoint: () => string,
  ) {}

  async open(
    request: WatchRequest,
    autoResume: boolean,
    signal: AbortSignal | undefined,
  ): Promise<KahunaWatch> {
    if ((request.durability ?? 'persistent') !== 'persistent') {
      throw KahunaError.keyValue(
        'A watch needs persistent keys: ephemeral writes are not replicated.',
        'watchUnsupportedOnEphemeral',
      );
    }

    const resumer = this.resumerFor(request, autoResume);
    let last: KeyValueResponseCode = 'mustRetry';

    for (let attempt = 0; attempt < WATCH_OPEN_ATTEMPTS; attempt++) {
      const url = this.nextEndpoint();
      const opening = await this.transport.openWatch(url, request, { signal });

      if (opening.stream !== null) {
        return new KahunaWatch(
          request.scope,
          [{ url, stream: opening.stream, partitions: opening.partitions }],
          opening.partitions,
          [url],
          resumer,
        );
      }

      last = opening.type;

      if (opening.type === 'doesNotExist' && opening.routes.length > 0) {
        const placed = await this.openOnReplicas(url, request, opening.routes, resumer, signal);
        if (placed !== null) return placed;
      } else if (!isTransientKeyValueCode(opening.type)) {
        throw KahunaError.keyValue(
          `The watch on '${request.scope.value}' was refused: ${opening.type}.`,
          opening.type,
        );
      }

      await delay(openBackoffMs(attempt), signal);
    }

    throw KahunaError.keyValue(
      `The watch on '${request.scope.value}' could not be opened: ${last}.`,
      'mustRetry',
    );
  }

  /**
   * Opens the watch on the replicas the routes name. The partitions the first node
   * hosts stay on it, and the others go to the fewest nodes that host them all,
   * picked greedily. Returns null when a node refused its part transiently (the
   * range map or the placement moved), after it closes every part already open.
   */
  private async openOnReplicas(
    firstUrl: string,
    request: WatchRequest,
    routes: readonly WatchRoute[],
    resumer: WatchResumer | null,
    signal: AbortSignal | undefined,
  ): Promise<KahunaWatch | null> {
    const plan = new Map<string, number[]>();
    const unplaced: { partitionId: number; candidates: string[] }[] = [];

    for (const route of routes) {
      if (route.hostedHere) {
        addToPlan(plan, firstUrl, route.partitionId);
        continue;
      }

      const candidates: string[] = [];
      for (const endpoint of route.endpoints) {
        const resolved = this.endpoints.resolve(endpoint);
        if (resolved !== null && !candidates.includes(resolved)) candidates.push(resolved);
      }

      if (candidates.length === 0) {
        throw KahunaError.keyValue(
          `No reachable replica hosts partition ${route.partitionId} of the watch on '${request.scope.value}'.`,
          'doesNotExist',
        );
      }

      unplaced.push({ partitionId: route.partitionId, candidates });
    }

    while (unplaced.length > 0) {
      // The endpoint that hosts the most unplaced partitions; ties go to the one listed first.
      let best = unplaced[0]!.candidates[0]!;
      let bestCount = 0;

      for (const { candidates } of unplaced) {
        for (const candidate of candidates) {
          const count = unplaced.filter((other) => other.candidates.includes(candidate)).length;
          if (count > bestCount) {
            best = candidate;
            bestCount = count;
          }
        }
      }

      for (let i = unplaced.length - 1; i >= 0; i--) {
        if (unplaced[i]!.candidates.includes(best)) {
          addToPlan(plan, best, unplaced[i]!.partitionId);
          unplaced.splice(i, 1);
        }
      }
    }

    const parts: WatchPart[] = [];
    const partitions = new Set<number>();

    try {
      for (const [url, part] of plan) {
        const opening = await this.transport.openWatch(url, { ...request, partitions: part }, { signal });

        if (opening.stream === null) {
          if (isTransientKeyValueCode(opening.type) || opening.type === 'doesNotExist') {
            await closeAll(parts);
            return null;
          }

          throw KahunaError.keyValue(
            `The watch on '${request.scope.value}' was refused by ${url}: ${opening.type}.`,
            opening.type,
          );
        }

        parts.push({ url, stream: opening.stream, partitions: opening.partitions });
        for (const partitionId of opening.partitions) partitions.add(partitionId);
      }
    } catch (error) {
      await closeAll(parts);
      throw error;
    }

    return new KahunaWatch(
      request.scope,
      parts,
      [...partitions].sort((a, b) => a - b),
      [...plan.keys()],
      resumer,
    );
  }

  private resumerFor(request: WatchRequest, autoResume: boolean): WatchResumer | null {
    if (!autoResume) return null;
    return (part, cursors, signal) => this.resumePart(request, part, cursors, signal);
  }

  /**
   * Reopens one node stream of a watch from the cursors, limited to its partitions:
   * on its node first, then on any node, and on a replica the refusing node names
   * when it does not host them. A permanent refusal ends the watch with it. A resume
   * that keeps failing transiently ends the watch with the frame that ended the
   * stream.
   */
  private async resumePart(
    request: WatchRequest,
    part: WatchPart,
    cursors: ReadonlyMap<number, HlcTimestamp>,
    signal: AbortSignal,
  ): Promise<{ part: WatchPart | null; failure: WatchFrame | null }> {
    const resume: WatchRequest = {
      ...request,
      partitions: part.partitions,
      resumeFrom: HLC_ZERO,
      resumePartitions: cursors,
    };
    let url = part.url;

    for (let attempt = 0; attempt < WATCH_OPEN_ATTEMPTS; attempt++) {
      let opening: WatchOpening;

      try {
        opening = await this.transport.openWatch(url, resume, { signal });
      } catch (error) {
        if (signal.aborted) throw error;
        // The node that served the stream may be the one that went away.
        opening = refusedWatchOpening('mustRetry');
      }

      if (opening.stream !== null) {
        return { part: { url, stream: opening.stream, partitions: opening.partitions }, failure: null };
      }

      const replica =
        opening.type === 'doesNotExist' && opening.routes.length > 0
          ? this.replicaOfAll(opening.routes, part.partitions)
          : null;

      if (replica !== null) url = replica;
      else if (isTransientKeyValueCode(opening.type) || opening.type === 'doesNotExist') {
        url = this.nextEndpoint();
      } else {
        return { part: null, failure: refusedWatchFrame(-1, opening.type) };
      }

      await delay(openBackoffMs(attempt), signal);
    }

    return { part: null, failure: null };
  }

  /** A reachable endpoint the routes name for every one of the partitions, or null. */
  private replicaOfAll(routes: readonly WatchRoute[], partitions: readonly number[]): string | null {
    let common: Set<string> | null = null;

    for (const partitionId of partitions) {
      const candidates = new Set<string>();

      for (const route of routes) {
        if (route.partitionId !== partitionId) continue;
        for (const endpoint of route.endpoints) {
          const resolved = this.endpoints.resolve(endpoint);
          if (resolved !== null) candidates.add(resolved);
        }
      }

      if (common === null) common = candidates;
      else for (const endpoint of common) if (!candidates.has(endpoint)) common.delete(endpoint);
    }

    if (common === null) return null;
    const first = common.values().next();
    return first.done ? null : first.value;
  }
}

function addToPlan(plan: Map<string, number[]>, url: string, partitionId: number): void {
  const part = plan.get(url);
  if (part === undefined) plan.set(url, [partitionId]);
  else part.push(partitionId);
}

async function closeAll(parts: readonly WatchPart[]): Promise<void> {
  for (const part of parts) await part.stream.close().catch(() => undefined);
}

function openBackoffMs(attempt: number): number {
  return Math.min(1_000, 50 << attempt) + Math.floor(Math.random() * 25);
}
