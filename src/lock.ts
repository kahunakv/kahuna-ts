import type { Durability } from './enums.js';
import { KahunaError } from './errors.js';
import { bytesToText } from './transport/codec.js';
import type { ExtendOutcome, LockAcquireResult, LockInfo } from './types.js';

/** The client operations a lock handle can call back into. */
export interface LockOwner {
  lockUrlFor(resource: string, servedFrom: string | null): string;
  extendLockAt(
    url: string,
    resource: string,
    owner: Uint8Array,
    expiryMs: number,
    durability: Durability,
    signal?: AbortSignal,
  ): Promise<ExtendOutcome>;
  releaseLockAt(
    url: string,
    resource: string,
    owner: Uint8Array,
    durability: Durability,
    signal?: AbortSignal,
  ): Promise<boolean>;
  getLockAt(
    url: string,
    resource: string,
    durability: Durability,
    signal?: AbortSignal,
  ): Promise<LockInfo | null>;
}

/**
 * A handle on a distributed lock.
 *
 * Release it with {@link KahunaLock.release}, or let `await using` do it: the
 * handle implements `Symbol.asyncDispose`. A handle whose acquisition failed
 * releases nothing on disposal.
 */
export class KahunaLock implements AsyncDisposable {
  private released = false;

  constructor(
    private readonly owner: LockOwner,
    readonly resource: string,
    readonly result: LockAcquireResult,
    private readonly ownerToken: Uint8Array | null,
    readonly durability: Durability,
    readonly fencingToken: number,
    private readonly servedFrom: string | null,
  ) {}

  /** True when this handle holds the lock. */
  get acquired(): boolean {
    return this.result === 'acquired';
  }

  /** The owner token the server recorded. Raises when the lock was not acquired. */
  get token(): Uint8Array {
    if (this.ownerToken === null) {
      throw KahunaError.lock('Lock was not acquired', 'errored');
    }
    return this.ownerToken;
  }

  /** The owner token as text, or an empty string when the lock was not acquired. */
  get tokenAsString(): string {
    return this.ownerToken === null ? '' : (bytesToText(this.ownerToken) ?? '');
  }

  /** Pushes the lock's expiry out by the given number of milliseconds. */
  async extend(expiryMs: number, options?: { signal?: AbortSignal }): Promise<ExtendOutcome> {
    if (!this.acquired || this.ownerToken === null) {
      throw KahunaError.lock('Lock was not acquired', 'errored');
    }

    return this.owner.extendLockAt(
      this.owner.lockUrlFor(this.resource, this.servedFrom),
      this.resource,
      this.ownerToken,
      expiryMs,
      this.durability,
      options?.signal,
    );
  }

  /** Reads what the server currently holds for this resource. */
  async info(options?: { signal?: AbortSignal }): Promise<LockInfo | null> {
    return this.owner.getLockAt(
      this.owner.lockUrlFor(this.resource, this.servedFrom),
      this.resource,
      this.durability,
      options?.signal,
    );
  }

  /** Releases the lock. Calling it twice is safe and the second call does nothing. */
  async release(): Promise<boolean> {
    if (this.released) return false;
    this.released = true;

    if (!this.acquired || this.ownerToken === null) return false;

    return this.owner.releaseLockAt(
      this.owner.lockUrlFor(this.resource, this.servedFrom),
      this.resource,
      this.ownerToken,
      this.durability,
    );
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.release();
  }

  toJSON(): Record<string, unknown> {
    return {
      resource: this.resource,
      acquired: this.acquired,
      fencingToken: this.fencingToken,
      owner: this.tokenAsString,
    };
  }
}
