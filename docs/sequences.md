# Sequences

A sequence gives out unique, increasing numbers, for example invoice numbers. Every
client in the cluster draws from the same sequence.

```ts
await client.createSequence('invoice-number', { initialValue: 1000, increment: 1 });

const next = await client.nextSequenceValue('invoice-number');
const block = await client.reserveSequenceRange('invoice-number', 100); // 100 values at once

// A retry with the same idempotency key gets the same value. It does not use a second value.
await client.nextSequenceValue('invoice-number', { idempotencyKey: requestId });
```

## Update a sequence

`updateSequence` changes the settings of a sequence, or restarts it at a new value.
It is the Kahuna form of `setval` or `ALTER SEQUENCE RESTART` in SQL databases.

```ts
const restarted = await client.updateSequence('invoice-number', {
  currentValue: 5000,
  blockSize: 1,
});
console.log(restarted.incarnation); // 1
```

- A field that you omit keeps its value.
- `removeMaxValue` and `removeBlockSize` remove those two settings.
- Each update increases `incarnation` by one.

An update takes about one block lease of the server to return (5 seconds by
default). The server waits until no node can give out a value from before the
update. During this time, the sequence refuses requests
with `mustRetry`.

If you set the current value lower, values from before and after the update can
repeat.
