# Locks

A distributed lock lets one process at a time, across all your machines, own a
resource. A lock has an expiry. If the holder crashes, the lock frees itself when
the expiry ends.

```ts
// One attempt. A lock that another holder owns comes back unacquired.
const lock = await client.acquireLock('resource', { expiry: 30_000 });

// Try again every 500 ms, for up to 5 seconds.
const waited = await client.acquireLock('resource', {
  expiry: 30_000,
  wait: 5_000,
  retry: 500,
});

if (lock.acquired) {
  console.log(lock.fencingToken);
  await lock.extend(30_000);
  const info = await lock.info();
  await lock.release();
}
```

## Durability

| Durability | Behavior |
| --- | --- |
| `'ephemeral'` | Lives in the memory of one node. A node restart loses it. |
| `'persistent'` | Replicated to several nodes through Raft, a consensus protocol. Survives a node restart. |

## Fencing token

The fencing token is a number that increases on every acquisition of the lock.

A holder can pause, for example during a long garbage collection. Its lock can
expire, and a second holder can acquire it. When the first holder wakes up, it
still believes that it owns the lock.

To stop this, send the fencing token with every write to the protected resource.
Make the resource refuse a write that carries a token lower than the last token
that it saw.
