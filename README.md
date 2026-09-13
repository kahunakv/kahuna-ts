# kahuna-client

TypeScript client for [Kahuna](https://github.com/kahunakv/kahuna): distributed
locks, a distributed key/value store and a distributed sequencer.

## Install

```bash
npm install kahuna-client
```

Node 20.11 or later. The package is ESM only.

## Quick start

```ts
import { KahunaClient } from 'kahuna-client';

await using client = new KahunaClient({
  endpoints: ['https://node1:8082', 'https://node2:8084', 'https://node3:8086'],
});

// Distributed lock
await using lock = await client.acquireLock('orders/327', { expiry: 30_000 });
if (lock.acquired) {
  await client.set('orders/327', 'shipped', { expiry: 60_000 });
}

// Key/value
const entry = await client.get('orders/327');
console.log(entry.success, entry.valueAsString());
```

`await using` releases the lock and closes the client at the end of the block.
Call `lock.release()` and `client.close()` yourself if your target does not
support explicit resource management.

## Choosing a transport

```ts
new KahunaClient({ endpoints, transport: 'grpc' }); // the default
new KahunaClient({ endpoints, transport: 'rest' });
```

Both transports implement the same operations and produce the same results.
gRPC has lower overhead per operation. REST needs no HTTP/2 and is easier to put
behind a proxy.

The `.proto` files ship inside the package and load at run time, so the gRPC
transport needs no `protoc` and no code generation step.

## Configuration

| Option | Default | Meaning |
| --- | --- | --- |
| `endpoints` | — | One node URL, or several. Required. |
| `transport` | `'grpc'` | `'grpc'`, `'rest'`, or your own `Transport`. |
| `routing` | `'auto'` | `'auto'`, `'roundRobin'`, `'learned'` or `'metadata'`. |
| `routingEndpointMap` | — | Maps advertised endpoints onto the URLs this client dials. |
| `allowUnlistedRoutingEndpoints` | `false` | Lets the client dial an endpoint no option named. |
| `routeCacheCapacity` | `4096` | How many learned routes the client holds. |
| `routeHintLifetimeMs` | `60_000` | How long a learned route is used before it is observed again. |
| `routingEndpointCooldownMs` | `5_000` | How long a failed endpoint is held out of routing. |
| `upgradeUrls` | `false` | Keeps a lock handle on the node that served its acquisition. |
| `defaultOperationTimeoutMs` | `30_000` | Deadline for a call whose caller passed no `AbortSignal`. |
| `allowInsecureCertificateValidation` | `false` | Development only. See below. |
| `trustedServerCertificateThumbprints` | `[]` | SHA-256 pins, in hexadecimal. |
| `useHttp2` | `false` | REST transport only. |
| `bearerToken` | `'xxx'` | REST transport only. |

### Routing

The client can learn where each resource lives, so a repeated operation on one key
goes straight to the node that owns it instead of landing on an arbitrary node and
being forwarded.

- `'auto'` — learned routing for a client given several endpoints, plain rotation
  for one given a single endpoint. This is the default.
- `'roundRobin'` — no routing cache at all.
- `'learned'` — learns a destination from the hint each response carries.
- `'metadata'` — reads the cluster's routing map and resolves a key it has never
  touched. The map is read in the background, never on an operation's own path.

Routing changes efficiency and never an operation's outcome. Whichever node
receives a request resolves the resource itself, so a stale route costs one
inter-node forward and nothing else.

A node advertises the address its peers route on, which is not always the address
your client dials. Map the two when they differ:

```ts
new KahunaClient({
  endpoints: ['https://localhost:8082'],
  routingEndpointMap: { 'https://10.0.0.2:8082': 'https://localhost:8082' },
});
```

A hint the map does not cover is refused, and the client stays on endpoint
rotation rather than dialling an address the operator never chose.

### TLS

`allowInsecureCertificateValidation` turns off certificate validation. Use it for
local development only; it leaves the connection open to a machine-in-the-middle
attack.

For a self-signed certificate in production, pin its digest instead:

```ts
new KahunaClient({
  endpoints,
  trustedServerCertificateThumbprints: ['3A7BD3E2360A3D29EEA436FCFB7E44C735D117C4'],
});
```

## Locks

```ts
// One attempt. A lock another holder owns comes back unacquired.
const lock = await client.acquireLock('resource', { expiry: 30_000 });

// Keep trying for up to 5 seconds, half a second apart.
const waited = await client.acquireLock('resource', {
  expiry: 30_000,
  wait: 5_000,
  retry: 500,
});

if (lock.acquired) {
  console.log(lock.fencingToken); // advances on every acquisition
  await lock.extend(30_000);
  const info = await lock.info();
  await lock.release();
}
```

A lock is `'ephemeral'` or `'persistent'`. An ephemeral lock lives in memory and
does not survive a node restart; a persistent one is replicated through Raft.

The fencing token advances on every acquisition, so a holder that woke up late can
be told apart from the current one. Pass it to whatever the lock protects.

## Key/value

```ts
await client.set('key', 'value', { expiry: 60_000 });
await client.set('key', 'value', { mode: 'ifNotExists' });
await client.setNoRevision('cache/key', 'value');   // no revision history

const entry = await client.get('key');
entry.success;            // false when the key holds no value
entry.value;              // Uint8Array | null
entry.valueAsString();
entry.valueAsNumber();
entry.valueAsBoolean();

await client.exists('key');
await client.extend('key', 60_000);
await client.delete('key');
await client.getRevision('key', 3);          // one archived revision
await client.get('key', { snapshotMs: cut }); // as of a wall-clock millisecond

// Conditional writes
await client.compareValueAndSet('key', 'next', 'current');
await client.compareRevisionAndSet('key', 'next', 7);

// Batches
await client.setMany([{ key: 'a', value: '1' }, { key: 'b', value: '2' }]);
await client.getMany([{ key: 'a' }, { key: 'b' }]);
await client.existsMany([{ key: 'a' }]);
await client.deleteMany(['a', 'b']);

// Scans
await client.getByBucket('orders');          // every key of one bucket
await client.scanAllByPrefix('orders');      // every key under a prefix
await client.getByRange({ prefix: 'orders', startKey: 'orders/100' });

for await (const row of client.scanByRange({ prefix: 'orders' }, { limit: 500 })) {
  console.log(row.key);
}
```

A value may be a string, a `Uint8Array`, or `null`. `null` means the key holds no
value, which is a different state from a key that holds zero bytes; both wires
carry the distinction and the client preserves it.

A batch spans several partitions, so the server answers in whatever order those
partitions reply. Match a row of `getMany`, `existsMany`, `setMany` or `deleteMany`
by its `key`, never by its position:

```ts
const rows = await client.getMany([{ key: 'a' }, { key: 'b' }]);
const byKey = new Map(rows.map((row) => [row.key, row]));
byKey.get('a')?.valueAsString();
```

## Transactions

Two forms. A script runs entirely on the server:

```ts
const script = client.loadScript(`
BEGIN (locking="optimistic")
 LET balance = GET @account
 IF to_int(balance) >= to_int(@amount) THEN
  SET @account to_int(balance) - to_int(@amount)
 END
 COMMIT
END
`);

const result = await script.run({
  parameters: [
    { key: '@account', value: 'accounts/17' },
    { key: '@amount', value: '25' },
  ],
});
```

`loadScript` hashes the script with BLAKE3 and sends the digest with every run, so
the server parses it once and reuses the parsed form.

A session holds the transaction open across several client round trips:

```ts
await using session = await client.beginTransaction({ locking: 'pessimistic' });

const balance = await session.get('accounts/17');
await session.set('accounts/17', balance.valueAsNumber() - 25);
await session.commit();
```

Disposing a session that is still pending rolls it back. `withTransaction` starts a
fresh transaction on every retryable conflict:

```ts
await client.withTransaction({ locking: 'optimistic' }, async (session) => {
  const balance = await session.get('accounts/17');
  await session.set('accounts/17', balance.valueAsNumber() - 25);
  await session.commit();
});
```

## Sequences

```ts
await client.createSequence('invoice-number', { initialValue: 1000, increment: 1 });

const next = await client.nextSequenceValue('invoice-number');
const block = await client.reserveSequenceRange('invoice-number', 100);

// A retry that carries the same key consumes one value, not two.
await client.nextSequenceValue('invoice-number', { idempotencyKey: requestId });
```

`updateSequence` changes the parameters of a sequence. It is the `setval` /
`ALTER SEQUENCE RESTART` operation. A field you omit keeps its value.
`removeMaxValue` and `removeBlockSize` remove those two settings.

```ts
const restarted = await client.updateSequence('invoice-number', {
  currentValue: 5000,
  blockSize: 1,
});
console.log(restarted.incarnation); // 1
```

An update takes about one server block lease to return (5 s by default). The
server holds its answer until no node can issue a value from the old stream. For
the same time, the sequence refuses allocations with `mustRetry`. Each update
increments `incarnation`. Values from two incarnations can overlap if you set the
current value lower.

## Cluster and operations

```ts
await client.getClusterMembership();
await client.getClusterPlacement();
await client.setReplicationFactor(partitionId, 3);
await client.leaveCluster({ nodeUrl: 'https://node3:8086' });

await client.registerKeyRange('orders');
await client.getRanges();
await client.splitRange('orders', 'orders/50000');
await client.mergeRanges();

await client.acquireSnapshotHold('reporting-job', floor, 60_000);
await client.renewSnapshotHold(holdId, 60_000);
await client.releaseSnapshotHold(holdId);
await client.getSnapshotFloor();

await client.takeFullBackup();
await client.takeIncrementalBackup(parentBackupId);
await client.listBackups();
await client.restore(leafBackupId, '/var/lib/kahuna/restored');
await client.collectBackupGarbage({ dryRun: true });
```

## Errors

Every protocol failure is a `KahunaError`. Read its `domain` before its `code`: the
three code families share several names.

```ts
import { KahunaError, isKeyValueCode } from 'kahuna-client';

try {
  await client.set('', 'value');
} catch (error) {
  if (error instanceof KahunaError) {
    console.log(error.domain, error.code); // 'keyValue' 'invalidInput'
  }
}

// Back off before you retry an admission refusal: the node is shedding load.
if (isKeyValueCode(error, 'admissionRefused')) await backOff();
```

An operation that the server answered is not an error. A write a condition
rejected, and a read of a key that holds no value, both come back as an entry whose
`success` is false.

## Cancellation

Every method takes an `AbortSignal`:

```ts
await client.get('key', { signal: AbortSignal.timeout(2_000) });
```

A call with no signal still gets `defaultOperationTimeoutMs`, so an unresponsive
node fails the call instead of hanging it.

## Numbers

Revisions, fencing tokens and sequence values cross the wire as 64-bit integers and
arrive as JavaScript numbers. Values above `Number.MAX_SAFE_INTEGER`
(9,007,199,254,740,991) lose precision. No Kahuna counter reaches that range in
practice, but a sequence whose `maxValue` you set above it would.

## Differences from the .NET client

- Names follow TypeScript convention: `acquireLock` rather than `GetOrCreateLock`,
  `KeyValueEntry` rather than `KahunaKeyValue`, `Transport` rather than
  `IKahunaCommunication`.
- Enumerations are string-literal unions (`'persistent'`, `'mustRetry'`) rather
  than numeric enums. The wire values are unchanged.
- Flags become an option: `set(key, value, { mode: 'ifNotExists' })` rather than a
  `KeyValueFlags` bit set.
- The gRPC transport sends unary calls. The .NET client multiplexes them over a
  batching bidirectional stream. Both reach the same server methods with the same
  semantics; the batcher is an efficiency optimisation this port does not carry.
- Cluster, key-range, snapshot-hold and backup operations are methods on the
  client, exactly as in the .NET client.

## Development

```bash
npm run typecheck
npm run build
npm test
```

The suite runs against a live cluster. Start the three-node Docker cluster first:

```bash
docker compose -f kahuna/docker/local.yml up -d
npm test
docker compose -f kahuna/docker/local.yml down
```

It expects `https://localhost:8082`, `https://localhost:8084` and
`https://localhost:8086`. Point `KAHUNA_TEST_ENDPOINTS` at a comma-separated list
to use another cluster, such as the one `scripts/run-cluster.sh` starts without
Docker:

```bash
KAHUNA_TEST_ENDPOINTS=https://localhost:8082 npm test
```

Two suites need no cluster at all: `test/value-parsing.test.ts` and
`test/placement-hash.test.ts`. The second pins the placement hash against vectors
taken from the server's own implementation, because a client that computes it
differently routes a subset of keys to the wrong node.

## License

MIT. See [LICENSE](LICENSE).
