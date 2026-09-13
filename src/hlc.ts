/**
 * A point in time from the Hybrid Logical Clock the cluster runs on.
 *
 * `physical` is milliseconds since the Unix epoch, `counter` separates two events
 * inside one millisecond, and `node` separates two nodes that produced the same
 * pair. The three fields together give a total order across the cluster.
 */
export interface HlcTimestamp {
  readonly node: number;
  readonly physical: number;
  readonly counter: number;
}

/** The timestamp every field of which is zero. It means "latest" on a read. */
export const HLC_ZERO: HlcTimestamp = Object.freeze({ node: 0, physical: 0, counter: 0 });

export function hlc(node: number, physical: number, counter: number): HlcTimestamp {
  return { node, physical, counter };
}

export function isHlcZero(value: HlcTimestamp | undefined): boolean {
  return value === undefined || (value.node === 0 && value.physical === 0 && value.counter === 0);
}

export function hlcEquals(left: HlcTimestamp, right: HlcTimestamp): boolean {
  return (
    left.node === right.node &&
    left.physical === right.physical &&
    left.counter === right.counter
  );
}

/**
 * Orders two timestamps by physical time, then counter, then node id.
 *
 * The node id is the last tie-breaker rather than an ignored field, so two
 * timestamps that differ only in it never compare as mutually "less than".
 */
export function compareHlc(left: HlcTimestamp, right: HlcTimestamp): number {
  if (left.physical !== right.physical) return left.physical < right.physical ? -1 : 1;
  if (left.counter !== right.counter) return left.counter < right.counter ? -1 : 1;
  if (left.node !== right.node) return left.node < right.node ? -1 : 1;
  return 0;
}

/** Builds the snapshot timestamp for a read "as of" a wall-clock millisecond. */
export function snapshotAt(physicalMs: number): HlcTimestamp {
  if (physicalMs === 0) return HLC_ZERO;
  // The highest counter reads every event committed within that millisecond.
  return { node: 0, physical: physicalMs, counter: 0xffff_ffff };
}

/** The JSON shape the REST transport puts on the wire. */
export interface HlcJson {
  n: number;
  l: number;
  c: number;
}

export function hlcToJson(value: HlcTimestamp | undefined): HlcJson {
  const source = value ?? HLC_ZERO;
  return { n: source.node, l: source.physical, c: source.counter };
}

export function hlcFromJson(value: HlcJson | null | undefined): HlcTimestamp {
  if (!value) return HLC_ZERO;
  return { node: value.n ?? 0, physical: value.l ?? 0, counter: value.c ?? 0 };
}
