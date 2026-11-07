# kahuna-client

TypeScript client for [Kahuna](https://github.com/kahunakv/kahuna), a distributed
store. Kahuna runs as a cluster of nodes, and gives your application:

- distributed locks,
- a distributed key/value store with transactions,
- distributed sequences,
- distributed rate limiters.

## Install

```bash
npm install kahuna-client
```

Requires Node 20.11 or later. The package is ESM only: use `import`, not `require`.

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

`endpoints` lists the URLs of the cluster nodes. All durations are in milliseconds.

`await using` releases the lock and closes the client at the end of the block. If
your runtime or compiler does not support `await using`, call `lock.release()` and
`client.close()` yourself.

## Documentation

| Topic | Contents |
| --- | --- |
| [Configuration](docs/configuration.md) | Client options, gRPC or REST, routing, TLS. |
| [Locks](docs/locks.md) | Acquire, extend and release locks. Fencing tokens. |
| [Key/value](docs/key-value.md) | Reads, writes, revisions, conditional writes, batches, scans. |
| [Transactions](docs/transactions.md) | Scripts, sessions, conflict policy, commit timestamps. |
| [Watches](docs/watches.md) | Stream changes to keys. Duplicates, resume, history holds. |
| [Sequences](docs/sequences.md) | Unique increasing numbers. |
| [Rate limiting](docs/rate-limiting.md) | Limits shared across replicas. HTTP middleware. |
| [Cluster and operations](docs/cluster.md) | Membership, key ranges, backups, admin key. |
| [Errors, cancellation and numbers](docs/errors.md) | `KahunaError`, timeouts, 64-bit values. |
| [Development](docs/development.md) | Build and test this package. |

## License

MIT. See [LICENSE](LICENSE).
