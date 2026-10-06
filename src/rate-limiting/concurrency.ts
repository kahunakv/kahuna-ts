import type { KahunaClient } from '../client.js';
import { RateLimitLease } from './lease.js';
import { KahunaRateLimiter, sleep } from './limiter.js';
import { validateConcurrency, type ConcurrencyLimiterOptions } from './options.js';
import {
  CONCURRENCY_ACQUIRE_SCRIPT,
  CONCURRENCY_RELEASE_SCRIPT,
  CONCURRENCY_RENEW_SCRIPT,
  selectScript,
  type RateLimiterScript,
} from './scripts.js';

/**
 * A concurrency limiter whose permits live in Kahuna: at most `permitLimit`
 * permits are held at once, across every process that names the same key. A
 * permit comes back when its lease is released.
 *
 * Every held lease is a key directly under `<key>/`, so this limiter owns that key
 * space: do not store anything else directly under it. The lease key expires
 * unless its holder renews it, which this process does every third of
 * `leaseDurationMs` until the lease is released. The permits of a process that
 * dies come back when its leases expire.
 *
 * A released lease gives its permits back in the background, so
 * {@link RateLimitLease.release} does not wait for the cluster. If that fails after
 * its retries, the lease key is left to expire, and the permits come back at the
 * expiry instead. A permit whose renewal fails for a whole lease period lapses
 * while its holder still runs, and another process can then take it.
 */
export class KahunaConcurrencyLimiter extends KahunaRateLimiter {
  readonly #acquireScript: RateLimiterScript;

  readonly #renewScript: RateLimiterScript;

  readonly #releaseScript: RateLimiterScript;

  readonly #limit: string;

  readonly #lease: string;

  readonly #renewIntervalMs: number;

  readonly #pollIntervalMs: number;

  /** The leases this process holds, by lease key, with the permits each one took. */
  readonly #heldLeases = new Map<string, number>();

  #renewalRunning = false;

  /** Releases still in flight, so {@link KahunaRateLimiter.close} can wait for them. */
  readonly #pendingReleases = new Set<Promise<void>>();

  constructor(client: KahunaClient, options: ConcurrencyLimiterOptions) {
    super(client, validateConcurrency(options), options.permitLimit);

    const leaseDurationMs = options.leaseDurationMs ?? 30_000;

    this.#acquireScript = selectScript(this.durability, CONCURRENCY_ACQUIRE_SCRIPT);
    this.#renewScript = selectScript(this.durability, CONCURRENCY_RENEW_SCRIPT);
    this.#releaseScript = selectScript(this.durability, CONCURRENCY_RELEASE_SCRIPT);
    this.#limit = String(options.permitLimit);
    this.#lease = String(leaseDurationMs);
    this.#renewIntervalMs = Math.floor(leaseDurationMs / 3);
    this.#pollIntervalMs = options.queuePollIntervalMs ?? 50;
  }

  protected override get hasHeldPermits(): boolean {
    return this.#heldLeases.size > 0;
  }

  protected override get queuePollIntervalMs(): number {
    return this.#pollIntervalMs;
  }

  protected override async attemptOnCluster(
    permits: number,
    signal: AbortSignal | undefined,
  ): Promise<RateLimitLease> {
    const leaseKey = `${this.key}/${crypto.randomUUID().replaceAll('-', '')}`;

    const { granted, value } = await this.decide(
      this.#acquireScript,
      [
        { key: '@bucket', value: this.key },
        { key: '@lease_key', value: leaseKey },
        { key: '@limit', value: this.#limit },
        { key: '@permits', value: String(permits) },
        { key: '@lease_ms', value: this.#lease },
      ],
      signal,
    );

    if (!granted) return this.refusal(value);

    this.observeAvailablePermits(value);

    // Zero permits asks whether permits are free, and writes no lease.
    if (permits === 0) return RateLimitLease.GRANTED;

    this.#hold(leaseKey, permits);

    return new RateLimitLease(true, null, () => this.#release(leaseKey));
  }

  protected override async waitForBackgroundWork(): Promise<void> {
    await Promise.all([...this.#pendingReleases]);
  }

  #hold(leaseKey: string, permits: number): void {
    this.#heldLeases.set(leaseKey, permits);

    if (this.#renewalRunning) return;

    this.#renewalRunning = true;
    void this.#renewLeases();
  }

  #release(leaseKey: string): void {
    if (!this.#heldLeases.delete(leaseKey)) return;

    const release: Promise<void> = this.#releaseOnCluster(leaseKey).finally(() => {
      this.#pendingReleases.delete(release);
    });

    this.#pendingReleases.add(release);
  }

  /**
   * Deletes the lease key, and then wakes the queue, because a waiter of this
   * process can take the permits now. Runs without a signal: closing the limiter
   * does not stop permits from coming back.
   */
  async #releaseOnCluster(leaseKey: string): Promise<void> {
    try {
      await this.runScript(this.#releaseScript, [{ key: '@lease_key', value: leaseKey }]);
    } catch {
      // The lease key expires on its own, and the permits come back then. Nothing
      // else can be done for a release that failed after its retries.
    } finally {
      this.wakeQueue();
    }
  }

  /**
   * Renews every held lease once per renewal interval, and stops when no lease is
   * held. A lease that expired before its renewal is not written again, because its
   * permits may already belong to another process.
   *
   * The renewal timer does not keep the process alive. A process that exits with
   * leases held gives their permits back when the leases expire.
   */
  async #renewLeases(): Promise<void> {
    const signal = this.closeSignal;

    try {
      while (true) {
        await sleep(this.#renewIntervalMs, signal, true);

        for (const [leaseKey, permits] of [...this.#heldLeases]) {
          if (!this.#heldLeases.has(leaseKey)) continue;

          try {
            await this.runScript(
              this.#renewScript,
              [
                { key: '@lease_key', value: leaseKey },
                { key: '@permits', value: String(permits) },
                { key: '@lease_ms', value: this.#lease },
              ],
              signal,
            );
          } catch (error) {
            // The next round tries again. The lease only lapses if every renewal
            // fails for a whole lease period.
            if (signal.aborted) throw error;
          }
        }

        if (this.#heldLeases.size === 0) {
          this.#renewalRunning = false;
          return;
        }
      }
    } catch {
      // Closing stops the renewals. Leases still held then expire on the cluster
      // unless their holders release them first.
      this.#renewalRunning = false;
    }
  }
}
