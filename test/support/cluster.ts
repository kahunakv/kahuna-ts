import { KahunaClient } from '../../src/index.js';

/**
 * The endpoints the suite talks to.
 *
 * They default to the three-node Docker cluster `docker/local.yml` starts. Point
 * `KAHUNA_TEST_ENDPOINTS` at a comma-separated list to use another cluster, such as
 * the single standalone node `scripts/run-standalone.sh` starts.
 */
export const ENDPOINTS: string[] = (
  process.env['KAHUNA_TEST_ENDPOINTS'] ?? 'https://localhost:8082,https://localhost:8084,https://localhost:8086'
)
  .split(',')
  .map((value) => value.trim())
  .filter((value) => value.length > 0);

export const SINGLE_ENDPOINT: string = ENDPOINTS[0]!;

/** Which wire protocol a test case exercises. */
export type TransportKind = 'grpc' | 'rest';

/** Whether a test case uses one endpoint or the whole pool. */
export type ClientShape = 'single' | 'pool';

export const TRANSPORTS: TransportKind[] = ['grpc', 'rest'];
export const CLIENT_SHAPES: ClientShape[] = ['single', 'pool'];
export const DURABILITIES = ['ephemeral', 'persistent'] as const;

const clients: KahunaClient[] = [];

/**
 * Builds a client for one combination, and registers it for teardown.
 *
 * The development cluster presents a self-signed certificate, so the suite turns
 * off certificate validation. Never do this against a real deployment.
 */
export function makeClient(
  transport: TransportKind,
  shape: ClientShape,
  extra: Partial<ConstructorParameters<typeof KahunaClient>[0]> = {},
): KahunaClient {
  const client = new KahunaClient({
    endpoints: shape === 'single' ? SINGLE_ENDPOINT : ENDPOINTS,
    transport,
    allowInsecureCertificateValidation: true,
    ...extra,
  });

  clients.push(client);
  return client;
}

/** Closes every client the suite built. Call it from an `afterAll` hook. */
export async function closeClients(): Promise<void> {
  await Promise.all(clients.splice(0).map((client) => client.close()));
}

/** A key no other test uses. */
export function randomKey(prefix = 'test-key'): string {
  return `${prefix}-${crypto.randomUUID().replaceAll('-', '')}`;
}

/** A lock name no other test uses. */
export function randomLockName(): string {
  return crypto.randomUUID().replaceAll('-', '').slice(0, 10);
}

/** Every combination of transport and client shape, as Vitest case labels. */
export function combinations(): [TransportKind, ClientShape][] {
  const cases: [TransportKind, ClientShape][] = [];
  for (const transport of TRANSPORTS) {
    for (const shape of CLIENT_SHAPES) cases.push([transport, shape]);
  }
  return cases;
}

/** Every combination of transport, client shape and durability. */
export function durableCombinations(): [TransportKind, ClientShape, 'ephemeral' | 'persistent'][] {
  const cases: [TransportKind, ClientShape, 'ephemeral' | 'persistent'][] = [];
  for (const [transport, shape] of combinations()) {
    for (const durability of DURABILITIES) cases.push([transport, shape, durability]);
  }
  return cases;
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
