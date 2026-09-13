import { createHash } from 'node:crypto';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { PeerCertificate } from 'node:tls';

import * as grpc from '@grpc/grpc-js';
import { loadSync, type Options as ProtoLoaderOptions } from '@grpc/proto-loader';

import type { SecurityOptions } from '../options.js';

const here = dirname(fileURLToPath(import.meta.url));

/**
 * Where the `.proto` files live.
 *
 * The package ships them beside the build output, so a consumer needs no code
 * generation step and no protoc.
 */
export const PROTO_ROOT = resolveProtoRoot();

function resolveProtoRoot(): string {
  // `dist/transport` at run time, `src/transport` under a source runner; the proto
  // directory sits at the package root in both layouts.
  return join(here, '..', '..', 'protos');
}

const PROTO_FILES = [
  'Communication/Grpc/Protos/locks.proto',
  'Communication/Grpc/Protos/keyvalues.proto',
  'Communication/Grpc/Protos/sequences.proto',
  'Communication/Grpc/Protos/cluster.proto',
  'Communication/Grpc/Protos/backups.proto',
];

const LOADER_OPTIONS: ProtoLoaderOptions = {
  // The proto files name their fields in PascalCase and the server reads them that
  // way, so the names are kept exactly as written rather than lower-cased.
  keepCase: true,
  // 64-bit fields become plain numbers. Every value Kahuna puts in one — a
  // revision, a fencing token, a millisecond clock — stays far inside the safe
  // integer range.
  longs: Number,
  // Absent fields stay absent, so a payload that was never set is distinguishable
  // from one set to zero bytes.
  defaults: false,
  oneofs: true,
};

export interface ServiceClients {
  readonly locker: grpc.Client;
  readonly keyValuer: grpc.Client;
  readonly sequencer: grpc.Client;
  readonly cluster: grpc.Client;
  readonly backups: grpc.Client;
}

type ServiceConstructor = new (
  address: string,
  credentials: grpc.ChannelCredentials,
  options?: grpc.ClientOptions,
) => grpc.Client;

interface LoadedServices {
  Locker: ServiceConstructor;
  KeyValuer: ServiceConstructor;
  Sequencer: ServiceConstructor;
  Cluster: ServiceConstructor;
  Backups: ServiceConstructor;
}

let cachedServices: LoadedServices | null = null;

function loadServices(): LoadedServices {
  if (cachedServices !== null) return cachedServices;

  const definition = loadSync(PROTO_FILES, { ...LOADER_OPTIONS, includeDirs: [PROTO_ROOT] });
  const loaded = grpc.loadPackageDefinition(definition) as unknown as LoadedServices;
  cachedServices = loaded;
  return loaded;
}

/**
 * Holds one set of service clients per node URL.
 *
 * Every client of one URL shares a channel, so the pool and its HTTP/2 connection
 * are set up once however many services talk to that node.
 */
export class GrpcChannelPool {
  private readonly clients = new Map<string, ServiceClients>();

  private readonly credentials: grpc.ChannelCredentials;

  private readonly insecureCredentials = grpc.credentials.createInsecure();

  constructor(security: SecurityOptions) {
    this.credentials = buildCredentials(security);
  }

  get(url: string): ServiceClients {
    const existing = this.clients.get(url);
    if (existing !== undefined) return existing;

    const { address, secure } = parseTarget(url);
    const credentials = secure ? this.credentials : this.insecureCredentials;
    const services = loadServices();
    const options: grpc.ClientOptions = {
      'grpc.max_receive_message_length': 64 * 1024 * 1024,
      'grpc.max_send_message_length': 64 * 1024 * 1024,
    };

    const built: ServiceClients = {
      locker: new services.Locker(address, credentials, options),
      keyValuer: new services.KeyValuer(address, credentials, options),
      sequencer: new services.Sequencer(address, credentials, options),
      cluster: new services.Cluster(address, credentials, options),
      backups: new services.Backups(address, credentials, options),
    };

    this.clients.set(url, built);
    return built;
  }

  close(): void {
    for (const services of this.clients.values()) {
      services.locker.close();
      services.keyValuer.close();
      services.sequencer.close();
      services.cluster.close();
      services.backups.close();
    }
    this.clients.clear();
  }
}

function parseTarget(url: string): { address: string; secure: boolean } {
  const parsed = new URL(url);
  const secure = parsed.protocol === 'https:';
  const port = parsed.port || (secure ? '443' : '80');
  return { address: `${parsed.hostname}:${port}`, secure };
}

function buildCredentials(security: SecurityOptions): grpc.ChannelCredentials {
  if (security.allowInsecureCertificateValidation) {
    return grpc.credentials.createSsl(null, null, null, {
      rejectUnauthorized: false,
      checkServerIdentity: () => undefined,
    });
  }

  const thumbprints = (security.trustedServerCertificateThumbprints ?? []).map((value) =>
    value.replace(/[^0-9a-fA-F]/g, '').toUpperCase(),
  );

  if (thumbprints.length === 0) return grpc.credentials.createSsl();

  // A pin replaces the host-name check: the thumbprint itself is the identity proof.
  return grpc.credentials.createSsl(null, null, null, {
    rejectUnauthorized: false,
    checkServerIdentity(_host: string, certificate: PeerCertificate): Error | undefined {
      const raw = certificate.raw;
      if (!raw) return new Error('Server presented no certificate to pin against');

      const digest = createHash('sha256').update(raw).digest('hex').toUpperCase();
      if (thumbprints.includes(digest)) return undefined;

      return new Error(`Server certificate thumbprint ${digest} is not trusted`);
    },
  });
}
