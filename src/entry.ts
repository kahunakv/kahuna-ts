import type { Durability } from './enums.js';
import { KahunaError } from './errors.js';
import { bytesToText } from './transport/codec.js';

/** The client operations an entry handle can call back into. */
export interface EntryOwner {
  extend(
    key: string,
    expiryMs: number,
    options?: { durability?: Durability; signal?: AbortSignal },
  ): Promise<KeyValueEntry>;
  delete(
    key: string,
    options?: { durability?: Durability; signal?: AbortSignal },
  ): Promise<KeyValueEntry>;
}

export interface KeyValueEntryInit {
  readonly key: string;
  readonly success: boolean;
  readonly revision: number;
  readonly durability: Durability;
  readonly timeElapsedMs: number;
  readonly value?: Uint8Array | null;
  readonly lastModified?: number;
}

/**
 * The answer to one key/value operation, plus the two follow-up verbs that need
 * nothing but the key it names.
 *
 * `success` reports the outcome of the operation that produced this entry: a write
 * that the condition rejected, or a read of a key that holds no value, is a
 * complete answer rather than a failure.
 */
export class KeyValueEntry {
  readonly key: string;

  readonly success: boolean;

  readonly revision: number;

  readonly value: Uint8Array | null;

  readonly durability: Durability;

  readonly timeElapsedMs: number;

  /** Physical millisecond of the last write. Zero when the server reported none. */
  readonly lastModified: number;

  constructor(
    private readonly owner: EntryOwner | null,
    init: KeyValueEntryInit,
  ) {
    this.key = init.key;
    this.success = init.success;
    this.revision = init.revision;
    this.value = init.value ?? null;
    this.durability = init.durability;
    this.timeElapsedMs = init.timeElapsedMs;
    this.lastModified = init.lastModified ?? 0;
  }

  /** The value decoded as UTF-8, or null when the key holds no value. */
  valueAsString(): string | null {
    return bytesToText(this.value);
  }

  /**
   * The value parsed as a base-10 integer.
   *
   * Surrounding whitespace and a leading sign are accepted. Anything else, and a
   * key that holds no value, raises {@link KahunaError}.
   */
  valueAsNumber(): number {
    const text = this.requireText();
    const trimmed = text.trim();

    if (!/^[+-]?\d+$/.test(trimmed)) {
      throw KahunaError.keyValue('Value cannot be cast to a number', 'invalidInput');
    }

    const parsed = Number(trimmed);
    if (!Number.isSafeInteger(parsed)) {
      throw KahunaError.keyValue('Value cannot be cast to a number', 'invalidInput');
    }
    return parsed;
  }

  /**
   * The value parsed as a boolean.
   *
   * Surrounding whitespace and any letter case are accepted. Anything but `true` or
   * `false`, and a key that holds no value, raises {@link KahunaError}.
   */
  valueAsBoolean(): boolean {
    const trimmed = this.requireText().trim().toLowerCase();
    if (trimmed === 'true') return true;
    if (trimmed === 'false') return false;
    throw KahunaError.keyValue('Value cannot be cast to a boolean', 'invalidInput');
  }

  /** Pushes this key's expiry out by the given number of milliseconds. */
  extend(expiryMs: number, options?: { signal?: AbortSignal }): Promise<KeyValueEntry> {
    return this.requireOwner().extend(this.key, expiryMs, {
      durability: this.durability,
      signal: options?.signal,
    });
  }

  /** Deletes this key. */
  delete(options?: { signal?: AbortSignal }): Promise<KeyValueEntry> {
    return this.requireOwner().delete(this.key, {
      durability: this.durability,
      signal: options?.signal,
    });
  }

  toJSON(): Record<string, unknown> {
    return {
      key: this.key,
      success: this.success,
      revision: this.revision,
      value: this.valueAsString(),
      durability: this.durability,
    };
  }

  private requireText(): string {
    const text = bytesToText(this.value);
    if (text === null) throw KahunaError.keyValue('Value cannot be cast', 'invalidInput');
    return text;
  }

  private requireOwner(): EntryOwner {
    if (this.owner === null) {
      throw KahunaError.keyValue('This entry is not bound to a client', 'errored');
    }
    return this.owner;
  }
}
