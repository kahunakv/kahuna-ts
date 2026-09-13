/**
 * Decides which advertised endpoint this client may dial.
 *
 * A response can name any address. Turning that name into a destination without a
 * check would let a server steer the client, and its credentials and TLS trust, at
 * an address the operator never chose.
 */
export class RoutingEndpointPolicy {
  /** Normalised configured URL to the exact string the client dials. */
  private readonly configured = new Map<string, string>();

  /** Normalised advertised URL to the exact string the client dials. */
  private readonly explicitMap = new Map<string, string>();

  private readonly resolvedCache = new Map<string, string | null>();

  constructor(
    configuredUrls: readonly string[],
    endpointMap: Readonly<Record<string, string>> | null,
    private readonly allowUnlisted: boolean,
  ) {
    for (const url of configuredUrls) {
      const normalized = normalize(url);
      if (normalized.length > 0) this.configured.set(normalized, url);
    }

    if (!endpointMap) return;

    for (const [advertised, target] of Object.entries(endpointMap)) {
      const from = normalize(advertised);
      const to = normalize(target);
      if (from.length === 0 || to.length === 0) continue;
      // The mapped target is canonicalised to the configured spelling when it names
      // one, so a mapping and a direct match end on the same string and therefore
      // the same connection pool.
      this.explicitMap.set(from, this.configured.get(to) ?? target);
    }
  }

  /** The URL to dial for this advertised endpoint, or null when it is refused. */
  resolve(advertised: string | null | undefined): string | null {
    if (!advertised) return null;

    const cached = this.resolvedCache.get(advertised);
    if (cached !== undefined) return cached;

    const answer = this.resolveUncached(advertised);
    this.resolvedCache.set(advertised, answer);
    return answer;
  }

  private resolveUncached(advertised: string): string | null {
    const normalized = normalize(advertised);
    if (normalized.length === 0) return null;

    const mapped = this.explicitMap.get(normalized);
    if (mapped !== undefined) return mapped;

    const canonical = this.configured.get(normalized);
    if (canonical !== undefined) return canonical;

    if (!this.allowUnlisted) return null;

    // An unlisted endpoint is dialled only when the caller opted in, and even then
    // only when it is a well-formed absolute HTTP or HTTPS URL.
    try {
      const parsed = new URL(advertised);
      return parsed.protocol === 'http:' || parsed.protocol === 'https:' ? advertised : null;
    } catch {
      return null;
    }
  }
}

function normalize(url: string | null | undefined): string {
  if (!url) return '';
  return url.replace(/\/+$/, '').toLowerCase();
}
