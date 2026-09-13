import { describe, expect, it } from 'vitest';

import { KahunaError, KeyValueEntry } from '../src/index.js';

/**
 * Locks in the contract of {@link KeyValueEntry.valueAsNumber} and
 * {@link KeyValueEntry.valueAsBoolean}: which inputs they accept, and which they
 * refuse. These cases need no cluster.
 */
function entry(value: string | null): KeyValueEntry {
  return new KeyValueEntry(null, {
    key: 'k',
    success: true,
    value: value === null ? null : new TextEncoder().encode(value),
    revision: 0,
    durability: 'ephemeral',
    timeElapsedMs: 0,
  });
}

describe('valueAsNumber', () => {
  it.each([
    ['0', 0],
    ['123', 123],
    ['-123', -123],
    ['+123', 123],
    [' 123 ', 123],
    ['9007199254740991', Number.MAX_SAFE_INTEGER],
    ['-9007199254740991', -Number.MAX_SAFE_INTEGER],
  ])('parses %j', (text, expected) => {
    expect(entry(text).valueAsNumber()).toBe(expected);
  });

  it.each([
    [''],
    ['abc'],
    ['12.5'],
    ['123abc'],
    ['99999999999999999999999999'],
  ])('refuses %j', (text) => {
    expect(() => entry(text).valueAsNumber()).toThrow(KahunaError);
  });

  it('refuses a key that holds no value', () => {
    expect(() => entry(null).valueAsNumber()).toThrow(KahunaError);
  });
});

describe('valueAsBoolean', () => {
  it.each([
    ['true', true],
    ['false', false],
    ['True', true],
    ['FALSE', false],
    [' true ', true],
  ])('parses %j', (text, expected) => {
    expect(entry(text).valueAsBoolean()).toBe(expected);
  });

  it.each([[''], ['1'], ['yes'], ['truex']])('refuses %j', (text) => {
    expect(() => entry(text).valueAsBoolean()).toThrow(KahunaError);
  });

  it('refuses a key that holds no value', () => {
    expect(() => entry(null).valueAsBoolean()).toThrow(KahunaError);
  });
});

describe('valueAsString', () => {
  it('returns null for a key that holds no value', () => {
    expect(entry(null).valueAsString()).toBeNull();
  });

  it('returns an empty string for a key that holds zero bytes', () => {
    expect(entry('').valueAsString()).toBe('');
  });
});
