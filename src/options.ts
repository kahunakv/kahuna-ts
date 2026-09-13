import type { RoutingMode } from './enums.js';
import type { Transport } from './transport/transport.js';

/** Transport-security settings shared by both wire protocols. */
export interface SecurityOptions {
  /**
   * Skips TLS server-certificate validation.
   *
   * For development and local testing only. It leaves the connection open to a
   * machine-in-the-middle attack.
   */
  allowInsecureCertificateValidation?: boolean;

  /**
   * Accepts only server certificates whose SHA-256 thumbprint, in hexadecimal, is
   * on this list. An empty list keeps the platform chain and host-name checks.
   * Ignored when {@link SecurityOptions.allowInsecureCertificateValidation} is on.
   */
  trustedServerCertificateThumbprints?: readonly string[];
}

/** Settings of a {@link import('./client.js').KahunaClient}. */
export interface KahunaClientOptions extends SecurityOptions {
  /** One or more node base URLs, such as `https://localhost:8082`. */
  endpoints: string | readonly string[];

  /** Which wire protocol to use. Defaults to `grpc`. */
  transport?: 'grpc' | 'rest' | Transport;

  /**
   * How the client picks the node to send an operation to.
   *
   * Defaults to `auto`: learned routing for a client given several endpoints, and
   * plain rotation for one given a single endpoint. Whichever mode is in force, the
   * node that receives the request resolves the resource itself, so the choice
   * changes efficiency and never an operation's outcome.
   */
  routing?: RoutingMode;

  /**
   * How many learned routes the client holds. Past it the least recently added are
   * dropped, so a workload over an unbounded key space costs bounded memory.
   */
  routeCacheCapacity?: number;

  /**
   * How long a learned route is used before it must be observed again, in
   * milliseconds. It bounds how long a client keeps choosing a destination that
   * leadership has moved away from. It does not make a route within it correct.
   */
  routeHintLifetimeMs?: number;

  /**
   * How long an endpoint is held out of routing after a transport failure, in
   * milliseconds. It stops later operations from queueing behind a node that is
   * down. It says nothing about whether the failed operation ran.
   */
  routingEndpointCooldownMs?: number;

  /** How long one routing-metadata map is kept before it is read again, in milliseconds. */
  routingMetadataLifetimeMs?: number;

  /**
   * Maps the endpoints the servers advertise onto the URLs this client dials, for a
   * deployment where the two differ. Both sides are compared without a trailing
   * slash and without case distinction.
   */
  routingEndpointMap?: Readonly<Record<string, string>>;

  /**
   * Lets the client dial an endpoint a response named that is neither a configured
   * URL nor a mapped one.
   *
   * Off by default. A response would otherwise be able to steer the client, and its
   * credentials and TLS trust, at an address the operator never chose.
   */
  allowUnlistedRoutingEndpoints?: boolean;

  /**
   * Keeps a lock handle on the node that served its acquisition, when that node is
   * one this client may dial.
   */
  upgradeUrls?: boolean;

  /**
   * Milliseconds to wait for an operation whose caller supplied no `AbortSignal`.
   * Set to 0 to wait forever.
   */
  defaultOperationTimeoutMs?: number;

  /** Bearer token sent on every REST request. */
  bearerToken?: string;

  /** Uses HTTP/2 for the REST transport. Off by default. */
  useHttp2?: boolean;
}

/** The settings after every default is applied. */
export interface ResolvedOptions {
  readonly endpoints: readonly string[];
  readonly routing: RoutingMode;
  readonly routeCacheCapacity: number;
  readonly routeHintLifetimeMs: number;
  readonly routingEndpointCooldownMs: number;
  readonly routingMetadataLifetimeMs: number;
  readonly routingEndpointMap: Readonly<Record<string, string>> | null;
  readonly allowUnlistedRoutingEndpoints: boolean;
  readonly upgradeUrls: boolean;
  readonly defaultOperationTimeoutMs: number;
  readonly bearerToken: string;
  readonly useHttp2: boolean;
  readonly allowInsecureCertificateValidation: boolean;
  readonly trustedServerCertificateThumbprints: readonly string[];
}

export function resolveOptions(options: KahunaClientOptions): ResolvedOptions {
  const endpoints =
    typeof options.endpoints === 'string' ? [options.endpoints] : [...options.endpoints];

  if (endpoints.length === 0) throw new TypeError('At least one endpoint is required');

  return {
    endpoints,
    routing: options.routing ?? 'auto',
    routeCacheCapacity: options.routeCacheCapacity ?? 4096,
    routeHintLifetimeMs: options.routeHintLifetimeMs ?? 60_000,
    routingEndpointCooldownMs: options.routingEndpointCooldownMs ?? 5_000,
    routingMetadataLifetimeMs: options.routingMetadataLifetimeMs ?? 60_000,
    routingEndpointMap: options.routingEndpointMap ?? null,
    allowUnlistedRoutingEndpoints: options.allowUnlistedRoutingEndpoints ?? false,
    upgradeUrls: options.upgradeUrls ?? false,
    defaultOperationTimeoutMs: options.defaultOperationTimeoutMs ?? 30_000,
    bearerToken: options.bearerToken ?? 'xxx',
    useHttp2: options.useHttp2 ?? false,
    allowInsecureCertificateValidation: options.allowInsecureCertificateValidation ?? false,
    trustedServerCertificateThumbprints: options.trustedServerCertificateThumbprints ?? [],
  };
}
