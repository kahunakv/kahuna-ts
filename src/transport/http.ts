import { createHash } from 'node:crypto';
import type { PeerCertificate } from 'node:tls';
import { Agent, request, type Dispatcher } from 'undici';

import type { SecurityOptions } from '../options.js';

/** What a single HTTP round trip produced. */
export interface HttpResponse {
  readonly status: number;
  readonly headers: Record<string, string | string[] | undefined>;
  readonly body: string;
}

export interface HttpRequestOptions {
  readonly method: 'GET' | 'POST';
  readonly url: string;
  readonly body?: Uint8Array;
  readonly bearerToken: string;
  readonly signal?: AbortSignal;
}

/**
 * Raised when a node answers with a status the client cannot use.
 *
 * The status and body are kept so a caller can tell a refusal apart from a
 * transport failure.
 */
export class HttpStatusError extends Error {
  override readonly name = 'HttpStatusError';

  constructor(
    readonly status: number,
    readonly body: string,
    readonly url: string,
    readonly headers: Record<string, string | string[] | undefined> = {},
  ) {
    super(`Request to ${url} failed with status ${status}`);
  }

  /** The first value of a response header, matched without case distinction. */
  header(name: string): string | null {
    const value = this.headers[name.toLowerCase()];
    if (Array.isArray(value)) return value[0] ?? null;
    return value ?? null;
  }
}

/**
 * One connection pool, shared by every request this client sends.
 *
 * TLS settings belong to the pool rather than to a request, so a client that pins
 * thumbprints cannot accidentally send one request under platform validation.
 */
export class HttpClient {
  private readonly agent: Agent;

  constructor(security: SecurityOptions, useHttp2: boolean) {
    this.agent = new Agent({
      allowH2: useHttp2,
      connect: buildConnectOptions(security),
    });
  }

  async close(): Promise<void> {
    await this.agent.close();
  }

  async send(options: HttpRequestOptions): Promise<HttpResponse> {
    const headers: Record<string, string> = {
      accept: 'application/json',
      authorization: `Bearer ${options.bearerToken}`,
    };
    if (options.body !== undefined) headers['content-type'] = 'application/json';

    const response = await request(options.url, {
      method: options.method,
      headers,
      body: options.body !== undefined ? Buffer.from(options.body) : undefined,
      dispatcher: this.agent as Dispatcher,
      signal: options.signal,
    });

    const body = await response.body.text();
    return { status: response.statusCode, headers: response.headers, body };
  }
}

function buildConnectOptions(security: SecurityOptions): Record<string, unknown> {
  if (security.allowInsecureCertificateValidation) {
    return { rejectUnauthorized: false };
  }

  const thumbprints = (security.trustedServerCertificateThumbprints ?? []).map((value) =>
    value.replace(/[^0-9a-fA-F]/g, '').toUpperCase(),
  );

  if (thumbprints.length === 0) return {};

  // Pinning replaces the host-name check but keeps the chain check off, exactly as
  // the .NET client does: the pin itself is the identity proof.
  return {
    rejectUnauthorized: false,
    checkServerIdentity(_host: string, certificate: PeerCertificate): Error | undefined {
      const raw = certificate.raw;
      if (!raw) return new Error('Server presented no certificate to pin against');

      const digest = createHash('sha256').update(raw).digest('hex').toUpperCase();
      if (thumbprints.includes(digest)) return undefined;

      return new Error(`Server certificate thumbprint ${digest} is not trusted`);
    },
  };
}

/** True when the status is worth repeating the request on. */
export function isRetriableStatus(status: number): boolean {
  return status === 408 || status === 429 || status === 502 || status === 503 || status === 504;
}
