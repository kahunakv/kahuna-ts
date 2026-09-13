import type { RoutingDomain, RoutingMode } from '../enums.js';
import type { RouteHint } from '../types.js';
import type { RouteSink } from '../transport/transport.js';
import { RouteCache, type RouteEntry } from './route-cache.js';
import { RoutingEndpointPolicy } from './endpoint-policy.js';
import { RoutingMetadataSnapshot } from './metadata-snapshot.js';

/** Counts of what the resolver did. Useful in tests and in diagnostics. */
export interface RoutingMetrics {
  cacheHits: number;
  cacheMisses: number;
  metadataHits: number;
  hintsLearned: number;
  hintsRejected: number;
  endpointsSuppressed: number;
  suppressedRoutesSkipped: number;
  metadataRefreshes: number;
  metadataRefreshesCoalesced: number;
  metadataRefreshFailures: number;
}

/** Reads one routing map from a node. */
export interface MetadataReader {
  getRoutingMetadata(
    url: string,
    keySpace: string | null,
    options?: { signal?: AbortSignal },
  ): Promise<import('../types.js').RoutingMetadata>;
}

const METADATA_READ_TIMEOUT_MS = 10_000;

/**
 * Chooses the node an operation is sent to, and learns from what comes back.
 *
 * A learned destination is an efficiency choice, never an authority claim. The node
 * that receives a request resolves the resource itself, so a stale answer here
 * costs one inter-node forward and cannot change the outcome.
 */
export class ClientRouteResolver implements RouteSink {
  private readonly suppressed = new Map<string, number>();

  private metadata: RoutingMetadataSnapshot | null = null;

  private metadataRefreshInFlight = false;

  readonly metrics: RoutingMetrics = {
    cacheHits: 0,
    cacheMisses: 0,
    metadataHits: 0,
    hintsLearned: 0,
    hintsRejected: 0,
    endpointsSuppressed: 0,
    suppressedRoutesSkipped: 0,
    metadataRefreshes: 0,
    metadataRefreshesCoalesced: 0,
    metadataRefreshFailures: 0,
  };

  constructor(
    private readonly mode: RoutingMode,
    private readonly cache: RouteCache,
    private readonly endpoints: RoutingEndpointPolicy,
    private readonly metadataReader: MetadataReader | null,
    private readonly bootstrapUrl: () => string,
    private readonly metadataLifetimeMs: number,
    private readonly endpointCooldownMs: number,
  ) {}

  /** How many routes the cache currently holds. */
  get cachedRouteCount(): number {
    return this.cache.size;
  }

  /** The cached entry for this resource, expired or not. For tests and diagnostics. */
  peek(domain: RoutingDomain, resource: string): RouteEntry | null {
    return this.cache.peek(RouteCache.keyOf(domain, resource));
  }

  /**
   * The endpoint to send an operation on this resource to, or null to fall back to
   * endpoint rotation.
   */
  select(domain: RoutingDomain, resource: string): string | null {
    if (!resource) return null;

    const key = RouteCache.keyOf(domain, resource);
    const entry = this.cache.get(key);

    if (entry !== null) {
      if (!this.isSuppressed(entry.endpoint)) {
        this.metrics.cacheHits++;
        return entry.endpoint;
      }
      // The endpoint this entry names has just failed. The entry itself is not
      // evicted: it may still be the right owner, and the cooldown is what lapses.
      this.metrics.suppressedRoutesSkipped++;
      return null;
    }

    this.metrics.cacheMisses++;
    if (this.mode !== 'metadata') return null;

    const snapshot = this.metadata;
    if (snapshot === null || !snapshot.isValidNow()) {
      // Discovery never sits on a request's own path. This operation goes out on
      // rotation now, and the map it starts loading serves the operations after it.
      this.startMetadataRefresh();
      return null;
    }

    const advertised = snapshot.resolve(domain, resource);
    if (advertised === null) return null;

    const endpoint = this.endpoints.resolve(advertised);
    if (endpoint === null || this.isSuppressed(endpoint)) return null;

    this.metrics.metadataHits++;
    return endpoint;
  }

  learn(
    domain: RoutingDomain,
    resource: string,
    hint: RouteHint | null,
    requestUrl: string,
  ): void {
    if (!resource || hint === null) return;

    if (hint.provenance === 'unknown') {
      this.metrics.hintsRejected++;
      return;
    }

    const resolved = this.endpoints.resolve(hint.endpoint);
    if (resolved === null) {
      this.metrics.hintsRejected++;
      return;
    }

    // The node answered, so it is reachable. That is exactly what its cooldown was
    // waiting to find out, and holding it out any longer would keep sending its own
    // traffic elsewhere.
    this.suppressed.delete(resolved);

    const key = RouteCache.keyOf(domain, resource);
    const current = this.cache.peek(key);

    if (current === null) {
      this.cache.set(key, resolved, hint.partitionId, hint.generation, hint.provenance);
      this.metrics.hintsLearned++;
      return;
    }

    const replaceable =
      Date.now() >= current.expiresAt ||
      current.endpoint === resolved ||
      current.endpoint === requestUrl ||
      this.isSuppressed(current.endpoint);

    if (!replaceable) {
      this.metrics.hintsRejected++;
      return;
    }

    this.cache.set(key, resolved, hint.partitionId, hint.generation, hint.provenance);
    this.metrics.hintsLearned++;
  }

  learnBatch(
    domain: RoutingDomain,
    items: readonly { key: string; routeIndex: number }[],
    table: readonly RouteHint[] | null,
    requestUrl: string,
  ): void {
    if (table === null || table.length === 0) return;

    for (const item of items) {
      // The index is 1-based; 0 means the server resolved no route for that item.
      if (item.routeIndex <= 0 || item.routeIndex > table.length) continue;
      if (!item.key) continue;
      this.learn(domain, item.key, table[item.routeIndex - 1]!, requestUrl);
    }
  }

  /**
   * The endpoint a handle should keep using, or null when it may not be dialled.
   *
   * The affinity still passes the endpoint policy and the failure cooldown, so a
   * handle cannot dial an address the operator never configured, and stops aiming at
   * a node that has just stopped answering.
   */
  tryUseAffinity(advertised: string | null | undefined): string | null {
    const resolved = this.endpoints.resolve(advertised);
    return resolved !== null && !this.isSuppressed(resolved) ? resolved : null;
  }

  reportEndpointFailure(url: string): void {
    if (!url) return;
    this.suppressed.set(url, Date.now() + this.endpointCooldownMs);
    this.metrics.endpointsSuppressed++;
  }

  /** Reads the routing map now. Returns false when the map was unusable. */
  async refreshMetadata(signal?: AbortSignal): Promise<boolean> {
    if (this.metadataReader === null) return false;

    this.metrics.metadataRefreshes++;

    let response: import('../types.js').RoutingMetadata;
    try {
      response = await this.metadataReader.getRoutingMetadata(this.bootstrapUrl(), null, { signal });
    } catch {
      this.metrics.metadataRefreshFailures++;
      return false;
    }

    const built = RoutingMetadataSnapshot.create(response, this.metadataLifetimeMs);
    if ('rejection' in built) {
      this.metrics.metadataRefreshFailures++;
      return false;
    }

    this.metadata = built.snapshot;
    return true;
  }

  private isSuppressed(endpoint: string): boolean {
    if (this.suppressed.size === 0) return false;

    const until = this.suppressed.get(endpoint);
    if (until === undefined) return false;
    if (Date.now() < until) return true;

    this.suppressed.delete(endpoint);
    return false;
  }

  private startMetadataRefresh(): void {
    if (this.metadataReader === null) return;

    if (this.metadataRefreshInFlight) {
      this.metrics.metadataRefreshesCoalesced++;
      return;
    }

    this.metadataRefreshInFlight = true;
    const timeout = AbortSignal.timeout(METADATA_READ_TIMEOUT_MS);

    void this.refreshMetadata(timeout)
      .catch(() => {
        this.metrics.metadataRefreshFailures++;
      })
      .finally(() => {
        this.metadataRefreshInFlight = false;
      });
  }
}

/** Resolves `auto` against the number of endpoints the client was given. */
export function resolveRoutingMode(configured: RoutingMode, endpointCount: number): RoutingMode {
  if (configured !== 'auto') return configured;
  // A single-endpoint client is left on rotation because it could act on almost no
  // hint: a hint names whichever node owns the resource, and only configured
  // endpoints are dialled, so hints naming the other nodes are refused anyway.
  return endpointCount > 1 ? 'learned' : 'roundRobin';
}
