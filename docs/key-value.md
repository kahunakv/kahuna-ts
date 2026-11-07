# Key/value

Every write of a key creates a new revision, a number that increases by one. Kahuna
keeps old revisions, so you can read a key as it was.

```ts
await client.set('key', 'value', { expiry: 60_000 });
await client.set('key', 'value', { mode: 'ifNotExists' });
await client.setNoRevision('cache/key', 'value');   // keeps no old revisions

const entry = await client.get('key');
entry.success;            // false when the key holds no value
entry.value;              // Uint8Array | null
entry.valueAsString();
entry.valueAsNumber();
entry.valueAsBoolean();

await client.exists('key');
await client.extend('key', 60_000);           // pushes the expiry forward
await client.delete('key');
await client.getRevision('key', 3);           // one old revision
await client.get('key', { snapshotMs: cut }); // the value at a wall-clock millisecond
```

## Values

A value is a string, a `Uint8Array`, or `null`.

`null` means that the key holds no value. A key that holds zero bytes is a
different state. The client keeps the two states apart.

## Conditional writes

A conditional write succeeds only when the key is still in the state you expect.

```ts
await client.compareValueAndSet('key', 'next', 'current'); // if the value is 'current'
await client.compareRevisionAndSet('key', 'next', 7);      // if the revision is 7
```

A rejected condition is not an error. The entry comes back with `success` false.

## Batches

```ts
await client.setMany([{ key: 'a', value: '1' }, { key: 'b', value: '2' }]);
await client.getMany([{ key: 'a' }, { key: 'b' }]);
await client.existsMany([{ key: 'a' }]);
await client.deleteMany(['a', 'b']);
```

A batch can touch keys on several nodes. The rows come back in the order that the
nodes answer, not in the order you sent. Match a row by its `key`:

```ts
const rows = await client.getMany([{ key: 'a' }, { key: 'b' }]);
const byKey = new Map(rows.map((row) => [row.key, row]));
byKey.get('a')?.valueAsString();
```

## Scans

A bucket is the part of a key before its last `/`. In `orders/327`, the bucket is
`orders`.

```ts
await client.getByBucket('orders');          // every key of one bucket
await client.scanAllByPrefix('orders');      // every key under a prefix
await client.getByRange({ prefix: 'orders', startKey: 'orders/100' });

// Reads the rows page by page, 500 rows per page.
for await (const row of client.scanByRange({ prefix: 'orders' }, { limit: 500 })) {
  console.log(row.key);
}
```
