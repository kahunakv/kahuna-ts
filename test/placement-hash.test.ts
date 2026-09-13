import { describe, expect, it } from 'vitest';

import { bucketOfKey, consistentHash } from '../src/index.js';
import { xxHash32 } from '../src/routing/hash.js';

const encoder = new TextEncoder();

/**
 * The placement hash decides which partition a client routes a key to. A client
 * that computes it differently from the server routes a subset of keys to the
 * wrong node, so these vectors are pinned rather than derived.
 */
describe('xxHash32', () => {
  it.each([
    ['', 0, 0x02cc5d05],
    ['a', 0, 0x550d7456],
    ['abc', 0, 0x32d153ff],
    ['Hello, world!', 0, 0x31b7405d],
    ['', 1, 0x0b2cb792],
  ])('hashes %j with seed %i', (text, seed, expected) => {
    expect(xxHash32(encoder.encode(text), seed)).toBe(expected);
  });
});

describe('consistentHash', () => {
  /**
   * Every expected value below came from `Kommander.HashUtils.ConsistentHash`, the
   * function the server itself uses.
   */
  const vectors: [string, number, number][] = [
    ['jepsen/register', 3, 2],
    ['jepsen/register/4', 3, 0],
    ['a', 3, 1],
    ['abc', 3, 2],
    ['orders/42', 3, 1],
    ['jepsen/register', 16, 13],
    ['jepsen/register/4', 16, 4],
    ['a', 16, 14],
    ['abc', 16, 2],
    ['orders/42', 16, 8],
    ['routing/abc', 16, 2],
    ['users|eu/1', 16, 5],
    ['x', 16, 7],
    ['some-random-key-0123456789', 16, 12],
    ['kahuna/sequences/counter', 16, 4],
    ['jepsen/register', 256, 13],
    ['jepsen/register/4', 256, 66],
    ['orders/42', 256, 182],
    ['jepsen/register', 1024, 372],
    ['jepsen/register/4', 1024, 701],
    ['kahuna/sequences/counter', 1024, 580],
  ];

  it.each(vectors)('maps %j onto %i buckets', (key, buckets, expected) => {
    expect(consistentHash(key, buckets)).toBe(expected);
  });

  it('puts every key in a single-bucket pool in bucket 0', () => {
    for (const [key] of vectors) expect(consistentHash(key, 1)).toBe(0);
  });

  it('refuses a pool of no buckets', () => {
    expect(() => consistentHash('k', 0)).toThrow(RangeError);
  });
});

describe('bucketOfKey', () => {
  it('hashes the key space rather than the whole key', () => {
    // "jepsen/register/4" lives in the key space "jepsen/register", so the two
    // resolve to the same bucket and therefore the same partition.
    expect(bucketOfKey('jepsen/register/4', 16)).toBe(consistentHash('jepsen/register', 16));
  });

  it('hashes the placement group rather than the whole key space', () => {
    // "users|eu" and "users|us" name the same group, so they share a partition.
    expect(bucketOfKey('users|eu/1', 16)).toBe(bucketOfKey('users|us/9', 16));
  });

  it('hashes a key with no separator as itself', () => {
    expect(bucketOfKey('x', 16)).toBe(consistentHash('x', 16));
  });
});
