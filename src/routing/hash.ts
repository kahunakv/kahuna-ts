/**
 * The placement hash the server publishes as `kahuna.placement-group-jump-xxh32-v1`.
 *
 * A client must reproduce it exactly or fall back to endpoint rotation: an
 * approximation resolves the wrong partition for a subset of keys, which is worse
 * than resolving none.
 */

/** Separator whose last occurrence in a key ends its key space. */
export const KEY_SPACE_SEPARATOR = '/';

/** Separator whose first occurrence in a key space ends its placement group. */
export const GROUP_SEPARATOR = '|';

/** Identifier of the algorithm this module implements. */
export const HASH_ALGORITHM_IDENTIFIER = 'kahuna.placement-group-jump-xxh32-v1';

const PRIME32_1 = 0x9e3779b1;
const PRIME32_2 = 0x85ebca77;
const PRIME32_3 = 0xc2b2ae3d;
const PRIME32_4 = 0x27d4eb2f;
const PRIME32_5 = 0x165667b1;

function rotl32(value: number, bits: number): number {
  return ((value << bits) | (value >>> (32 - bits))) >>> 0;
}

function mul32(a: number, b: number): number {
  // Split b so neither partial product leaves the exact-integer range.
  return (((a * (b >>> 16)) << 16) + a * (b & 0xffff)) >>> 0;
}

/** xxHash32 over the exact bytes, with the given seed. */
export function xxHash32(input: Uint8Array, seed: number): number {
  const length = input.length;
  const view = new DataView(input.buffer, input.byteOffset, input.byteLength);
  let index = 0;
  let hash: number;

  if (length >= 16) {
    const limit = length - 16;
    let v1 = (seed + PRIME32_1 + PRIME32_2) >>> 0;
    let v2 = (seed + PRIME32_2) >>> 0;
    let v3 = seed >>> 0;
    let v4 = (seed - PRIME32_1) >>> 0;

    do {
      v1 = mul32(rotl32((v1 + mul32(view.getUint32(index, true), PRIME32_2)) >>> 0, 13), PRIME32_1);
      index += 4;
      v2 = mul32(rotl32((v2 + mul32(view.getUint32(index, true), PRIME32_2)) >>> 0, 13), PRIME32_1);
      index += 4;
      v3 = mul32(rotl32((v3 + mul32(view.getUint32(index, true), PRIME32_2)) >>> 0, 13), PRIME32_1);
      index += 4;
      v4 = mul32(rotl32((v4 + mul32(view.getUint32(index, true), PRIME32_2)) >>> 0, 13), PRIME32_1);
      index += 4;
    } while (index <= limit);

    hash = (rotl32(v1, 1) + rotl32(v2, 7) + rotl32(v3, 12) + rotl32(v4, 18)) >>> 0;
  } else {
    hash = (seed + PRIME32_5) >>> 0;
  }

  hash = (hash + length) >>> 0;

  while (index + 4 <= length) {
    hash = mul32(rotl32((hash + mul32(view.getUint32(index, true), PRIME32_3)) >>> 0, 17), PRIME32_4);
    index += 4;
  }

  while (index < length) {
    hash = mul32(rotl32((hash + mul32(input[index]!, PRIME32_5)) >>> 0, 11), PRIME32_1);
    index += 1;
  }

  hash = mul32(hash ^ (hash >>> 15), PRIME32_2);
  hash = mul32(hash ^ (hash >>> 13), PRIME32_3);
  return (hash ^ (hash >>> 16)) >>> 0;
}

const JUMP_MULTIPLIER = 2862933555777941757n;
const MASK_64 = 0xffff_ffff_ffff_ffffn;

/** Jump consistent hash: maps a 64-bit key onto `[0, buckets)`. */
export function jumpConsistentHash(key: bigint, buckets: number): number {
  let state = key & MASK_64;
  let b = -1;
  let j = 0;

  while (j < buckets) {
    b = j;
    state = (state * JUMP_MULTIPLIER + 1n) & MASK_64;
    const denominator = Number((state >> 33n) + 1n);
    j = Math.floor((b + 1) * (2147483648.0 / denominator));
  }

  return b;
}

const encoder = new TextEncoder();

/**
 * Maps a string onto one of `numBuckets` buckets.
 *
 * Two fixed seeds give two 32-bit digests, which combine into the 64-bit key the
 * jump hash consumes. The seeds are part of the contract.
 */
export function consistentHash(key: string, numBuckets: number): number {
  if (numBuckets <= 0) throw new RangeError('numBuckets must be greater than 0');

  const bytes = encoder.encode(key);
  const hash1 = xxHash32(bytes, 0xaaaaaaaa);
  const hash2 = xxHash32(bytes, 0x55555555);
  const combined = (BigInt(hash1) << 32n) | BigInt(hash2);
  return jumpConsistentHash(combined, numBuckets);
}

/** The key space of a key: its prefix up to, and not including, the last separator. */
export function keySpaceOf(key: string): string {
  const separator = key.lastIndexOf(KEY_SPACE_SEPARATOR);
  return separator < 0 ? key : key.slice(0, separator);
}

/** The placement group of a key space: its prefix up to the first group separator. */
export function groupOf(keySpace: string): string {
  const separator = keySpace.indexOf(GROUP_SEPARATOR);
  return separator < 0 ? keySpace : keySpace.slice(0, separator);
}

/** The bucket a key falls in, for a hash-routed key space. */
export function bucketOfKey(key: string, poolSize: number): number {
  const group = groupOf(keySpaceOf(key));
  return consistentHash(group.length === key.length ? key : group, poolSize);
}
