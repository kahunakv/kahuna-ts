import type { ValueInput } from '../types.js';

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Turns whatever the caller passed as a value into the bytes the wire carries. */
export function toBytes(value: ValueInput | undefined): Uint8Array | null {
  if (value === null || value === undefined) return null;
  if (typeof value === 'string') return encoder.encode(value);
  return value;
}

export function bytesToText(value: Uint8Array | null | undefined): string | null {
  if (value === null || value === undefined) return null;
  return decoder.decode(value);
}

/**
 * Encodes a payload for JSON.
 *
 * `null` stays `null` and any array, the empty one included, becomes a base64
 * string. This keeps apart a key that holds no value and a key that holds zero
 * bytes, which is the same distinction the gRPC wire carries in a presence flag.
 */
export function payloadToJson(value: Uint8Array | null): string | null {
  if (value === null) return null;
  return Buffer.from(value).toString('base64');
}

export function payloadFromJson(value: string | null | undefined): Uint8Array | null {
  if (value === null || value === undefined) return null;
  return new Uint8Array(Buffer.from(value, 'base64'));
}

/** Encodes a field that has no presence flag, such as a lock owner. */
export function requiredPayloadToJson(value: Uint8Array): string {
  return Buffer.from(value).toString('base64');
}

/** Builds a 32-byte ASCII lock-owner token from a fresh UUID. */
export function newLockOwner(): Uint8Array {
  return encoder.encode(crypto.randomUUID().replaceAll('-', ''));
}

export function textToBytes(value: string): Uint8Array {
  return encoder.encode(value);
}
