import type { RoutingDomain } from '../enums.js';
import type { RoutingMetadata } from '../types.js';
import {
  GROUP_SEPARATOR,
  HASH_ALGORITHM_IDENTIFIER,
  KEY_SPACE_SEPARATOR,
  bucketOfKey,
} from './hash.js';

const SUPPORTED_SCHEMA_VERSION = 1;

interface RangeInterval {
  readonly startKey: string | null;
  readonly endKey: string | null;
  readonly partitionId: number;
}

/** Why a metadata map was refused. */
export type SnapshotRejection =
  | 'not_initialized'
  | 'incoherent'
  | 'unsupported_schema'
  | 'unsupported_hash'
  | 'unsupported_sequence_key';

/**
 * One coherent view of the cluster's routing map.
 *
 * The map is published whole. A reader sees either the previous map or this one,
 * never a mixture, so a split that landed between two key spaces cannot be
 * observed half-applied.
 */
export class RoutingMetadataSnapshot {
  private constructor(
    private readonly hashPoolSize: number,
    private readonly hashPartitionOffset: number,
    private readonly sequenceKeyPrefix: string,
    private readonly reservedKeyPrefix: string,
    private readonly rangedSpaces: Map<string, RangeInterval[]>,
    private readonly leaders: Map<number, string>,
    readonly expiresAt: number,
  ) {}

  isValidNow(): boolean {
    return Date.now() < this.expiresAt;
  }

  /**
   * Builds a snapshot, or explains why the response is not usable.
   *
   * Every refusal leaves the client on its existing endpoint selection, which is
   * always correct and only less efficient.
   */
  static create(
    response: RoutingMetadata,
    lifetimeMs: number,
  ): { snapshot: RoutingMetadataSnapshot } | { rejection: SnapshotRejection } {
    if (!response.initialized) return { rejection: 'not_initialized' };
    if (!response.coherent) return { rejection: 'incoherent' };
    if (response.schemaVersion !== SUPPORTED_SCHEMA_VERSION) {
      return { rejection: 'unsupported_schema' };
    }

    // The separators are part of the function. A server that places by another key-space
    // or group boundary would be mis-hashed by this client for exactly the keys that
    // contain them.
    if (
      response.hashAlgorithm !== HASH_ALGORITHM_IDENTIFIER ||
      response.prefixSeparator !== KEY_SPACE_SEPARATOR ||
      response.groupSeparator !== GROUP_SEPARATOR ||
      response.hashPoolSize <= 0
    ) {
      return { rejection: 'unsupported_hash' };
    }

    const ranged = new Map<string, RangeInterval[]>();
    for (const space of response.keySpaces) {
      if (space.routingMode !== 'KeyRange') continue;

      // A ranged space with no intervals covers nothing. Recording it as
      // ranged-but-empty is what makes a key in it resolve to "unknown" rather than
      // fall through to the hash answer, which is a different partition entirely.
      const intervals: RangeInterval[] = space.ranges.map((range) => ({
        startKey: range.startKey,
        endKey: range.endKey,
        partitionId: range.partitionId,
      }));

      // The server sends them ordered. The client sorts rather than trusting that
      // order: a binary search over an unsorted array answers wrongly instead of not
      // at all.
      intervals.sort((a, b) => compareStarts(a.startKey, b.startKey));
      ranged.set(space.keySpace, intervals);
    }

    const leaders = new Map<number, string>();
    for (const leader of response.leaders) {
      // An unknown leader is reported with an empty endpoint. Recording it would make
      // the client believe it knows a destination it does not.
      if (leader.endpoint) leaders.set(leader.partitionId, leader.endpoint);
    }

    // The storage-key rule is applied as a prefix, so a format that puts anything
    // after the name is one this client cannot honour.
    let sequencePrefix = '';
    if (response.sequenceStorageKeyFormat.endsWith('{0}')) {
      sequencePrefix = response.sequenceStorageKeyFormat.slice(0, -3);
    } else if (response.sequenceStorageKeyFormat.length > 0) {
      return { rejection: 'unsupported_sequence_key' };
    }

    return {
      snapshot: new RoutingMetadataSnapshot(
        response.hashPoolSize,
        response.hashPartitionOffset,
        sequencePrefix,
        response.reservedKeyPrefix,
        ranged,
        leaders,
        Date.now() + lifetimeMs,
      ),
    };
  }

  /** The endpoint that owns this resource, or null when the map cannot say. */
  resolve(domain: RoutingDomain, resource: string): string | null {
    const partitionId = this.resolvePartition(domain, resource);
    if (partitionId === null) return null;
    return this.leaders.get(partitionId) ?? null;
  }

  resolvePartition(domain: RoutingDomain, resource: string): number | null {
    if (!resource) return null;

    // Server-managed records are routed by the subsystem that owns them. A client
    // neither reads nor caches routes under that prefix.
    if (
      this.reservedKeyPrefix.length > 0 &&
      domain !== 'sequence' &&
      resource.startsWith(this.reservedKeyPrefix)
    ) {
      return null;
    }

    switch (domain) {
      case 'keyValue':
        return this.resolveKeyValuePartition(resource);
      case 'lock':
        // Locks are hash-routed only. The lock subsystem never consults the range map,
        // so resolving a lock through range descriptors answers a different partition.
        return this.hashPartition(resource);
      case 'sequence':
        // A sequence routes by the partition of its storage key, not by a hash of the
        // bare name, so the storage-key rule the server published is applied first.
        if (this.sequenceKeyPrefix.length === 0) return null;
        return this.hashPartition(this.sequenceKeyPrefix + resource);
      default:
        return null;
    }
  }

  private resolveKeyValuePartition(key: string): number | null {
    const separator = key.lastIndexOf(KEY_SPACE_SEPARATOR);
    const keySpace = separator < 0 ? key : key.slice(0, separator);

    const intervals = this.rangedSpaces.get(keySpace);
    if (intervals === undefined) return this.hashPartition(key);

    // Rightmost interval whose start is at or below the key, then a containment
    // check. These are the two steps the server's own router takes.
    let low = 0;
    let high = intervals.length - 1;
    let found = -1;

    while (low <= high) {
      const mid = low + ((high - low) >> 1);
      if (startAtOrBelow(intervals[mid]!.startKey, key)) {
        found = mid;
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }

    if (found < 0) return null;
    const interval = intervals[found]!;
    return contains(interval, key) ? interval.partitionId : null;
  }

  private hashPartition(key: string): number {
    return this.hashPartitionOffset + bucketOfKey(key, this.hashPoolSize);
  }
}

function startAtOrBelow(start: string | null, key: string): boolean {
  return start === null || compareOrdinal(start, key) <= 0;
}

function contains(interval: RangeInterval, key: string): boolean {
  if (interval.startKey !== null && compareOrdinal(key, interval.startKey) < 0) return false;
  return interval.endKey === null || compareOrdinal(key, interval.endKey) < 0;
}

function compareStarts(left: string | null, right: string | null): number {
  if (left === null) return right === null ? 0 : -1;
  if (right === null) return 1;
  return compareOrdinal(left, right);
}

/**
 * Compares two strings by UTF-16 code unit, the way the server compares range
 * bounds. `String.prototype.localeCompare` would order them by locale instead.
 */
export function compareOrdinal(left: string, right: string): number {
  if (left === right) return 0;
  return left < right ? -1 : 1;
}
