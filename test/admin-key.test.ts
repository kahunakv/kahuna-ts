import { createServer, type IncomingHttpHeaders, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';

import * as grpc from '@grpc/grpc-js';
import { loadSync } from '@grpc/proto-loader';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { GrpcTransport, KahunaError, RestTransport } from '../src/index.js';
import { PROTO_ROOT } from '../src/transport/grpc-channels.js';

// These tests need no cluster: each one talks to a stub node that records the
// request headers.

const ADMIN_KEY = 'test-admin-key-0123456789abcdef';

describe('admin key over REST', () => {
  let server: Server;
  let url: string;
  const seen: { path: string; headers: IncomingHttpHeaders }[] = [];
  let refuseAdmin = false;

  beforeAll(async () => {
    server = createServer((request, response) => {
      seen.push({ path: request.url ?? '', headers: request.headers });
      request.resume();
      request.on('end', () => {
        const path = request.url ?? '';
        response.setHeader('content-type', 'application/json');
        if (path === '/v1/backups') {
          response.end('[]');
          return;
        }
        if (refuseAdmin) {
          response.statusCode = 401;
          response.end();
          return;
        }
        response.end(
          JSON.stringify({ left: false, drained: false, outcome: 'Evicted', retryable: true }),
        );
      });
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  });

  afterAll(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  it('sends the admin key with an admin call only', async () => {
    const transport = new RestTransport({ adminApiKey: ADMIN_KEY });
    try {
      seen.length = 0;
      const result = await transport.leaveCluster(url);
      expect(result.outcome).toBe('Evicted');
      await transport.listBackups(url);

      expect(seen.map((entry) => entry.path)).toEqual(['/v1/cluster/leave', '/v1/backups']);
      expect(seen[0]!.headers['x-kahuna-admin-key']).toBe(ADMIN_KEY);
      expect(seen[1]!.headers['x-kahuna-admin-key']).toBeUndefined();
    } finally {
      await transport.close();
    }
  });

  it('sends no admin key when none is configured', async () => {
    const transport = new RestTransport();
    try {
      seen.length = 0;
      await transport.leaveCluster(url);
      expect(seen[0]!.headers['x-kahuna-admin-key']).toBeUndefined();
    } finally {
      await transport.close();
    }
  });

  it('names the admin key when the node refuses with 401', async () => {
    const transport = new RestTransport();
    refuseAdmin = true;
    try {
      const error = await transport.leaveCluster(url).catch((caught: unknown) => caught);
      expect(error).toBeInstanceOf(KahunaError);
      expect((error as KahunaError).message).toContain('/v1/cluster/leave');
      expect((error as KahunaError).message).toContain('adminApiKey');
    } finally {
      refuseAdmin = false;
      await transport.close();
    }
  });
});

describe('admin key over gRPC', () => {
  let server: grpc.Server;
  let url: string;
  const seen: { method: string; key: grpc.MetadataValue[] }[] = [];
  let refuseAdmin = false;

  beforeAll(async () => {
    const definition = loadSync(
      ['Communication/Grpc/Protos/cluster.proto', 'Communication/Grpc/Protos/backups.proto'],
      { keepCase: true, longs: Number, defaults: false, oneofs: true, includeDirs: [PROTO_ROOT] },
    );
    const loaded = grpc.loadPackageDefinition(definition) as unknown as Record<
      string,
      { service: grpc.ServiceDefinition }
    >;

    server = new grpc.Server();
    server.addService(loaded['Cluster']!.service, {
      Leave: (
        call: grpc.ServerUnaryCall<unknown, unknown>,
        callback: grpc.sendUnaryData<unknown>,
      ) => {
        seen.push({ method: 'Leave', key: call.metadata.get('x-kahuna-admin-key') });
        if (refuseAdmin) {
          callback({ code: grpc.status.UNAUTHENTICATED, details: 'A valid admin key is required.' });
          return;
        }
        // 8 is LEAVE_CLUSTER_OUTCOME_EVICTED.
        callback(null, { Left: false, Drained: false, Outcome: 8, Retryable: true });
      },
    });
    server.addService(loaded['Backups']!.service, {
      ListBackups: (
        call: grpc.ServerUnaryCall<unknown, unknown>,
        callback: grpc.sendUnaryData<unknown>,
      ) => {
        seen.push({ method: 'ListBackups', key: call.metadata.get('x-kahuna-admin-key') });
        callback(null, { Backups: [] });
      },
    });

    const port = await new Promise<number>((resolve, reject) =>
      server.bindAsync('127.0.0.1:0', grpc.ServerCredentials.createInsecure(), (error, bound) =>
        error ? reject(error) : resolve(bound),
      ),
    );
    url = `http://127.0.0.1:${port}`;
  });

  afterAll(() => {
    server.forceShutdown();
  });

  it('sends the admin key with an admin call only', async () => {
    const transport = new GrpcTransport({ adminApiKey: ADMIN_KEY });
    try {
      seen.length = 0;
      const result = await transport.leaveCluster(url);
      expect(result.outcome).toBe('Evicted');
      await transport.listBackups(url);

      expect(seen).toEqual([
        { method: 'Leave', key: [ADMIN_KEY] },
        { method: 'ListBackups', key: [] },
      ]);
    } finally {
      await transport.close();
    }
  });

  it('sends no admin key when none is configured', async () => {
    const transport = new GrpcTransport();
    try {
      seen.length = 0;
      await transport.leaveCluster(url);
      expect(seen).toEqual([{ method: 'Leave', key: [] }]);
    } finally {
      await transport.close();
    }
  });

  it('names the admin key when the node refuses with UNAUTHENTICATED', async () => {
    const transport = new GrpcTransport();
    refuseAdmin = true;
    try {
      const error = await transport.leaveCluster(url).catch((caught: unknown) => caught);
      expect(error).toBeInstanceOf(KahunaError);
      expect((error as KahunaError).message).toContain('Leave');
      expect((error as KahunaError).message).toContain('adminApiKey');
    } finally {
      refuseAdmin = false;
      await transport.close();
    }
  });
});
