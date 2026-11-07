# Transactions

A transaction groups reads and writes. All its writes apply, or none apply.

Kahuna has two forms of transaction:

- A **script** runs on the server in one round trip.
- A **session** stays open across several round trips from the client.

Both forms take a locking mode:

| Locking | Behavior |
| --- | --- |
| `'optimistic'` | Takes no lock when it reads. Conflicts show at commit. Best when conflicts are rare. |
| `'pessimistic'` | Takes an exclusive lock on each key that it reads or writes. Best when conflicts are frequent. |

An optimistic session does not check the keys that it read, by default. Set
`readValidation: 'trackAndValidate'` to make the commit fail when another writer
changed one of those keys.

## Scripts

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

`loadScript` sends a BLAKE3 hash of the script with every run. The server parses
the script once and reuses the result.

## Sessions

```ts
await using session = await client.beginTransaction({ locking: 'pessimistic' });

const balance = await session.get('accounts/17');
await session.set('accounts/17', balance.valueAsNumber() - 25);
await session.commit();
```

When `await using` disposes a session that did not commit, the session rolls back.

`withTransaction` runs your function and starts a new transaction on each conflict
that a retry can fix:

```ts
await client.withTransaction({ locking: 'optimistic' }, async (session) => {
  const balance = await session.get('accounts/17');
  await session.set('accounts/17', balance.valueAsNumber() - 25);
  await session.commit();
});
```

A session can write a limited number of keys and hold a limited number of locks.
See [Mutation limit](errors.md#mutation-limit).

## Conflict policy

A write intent is the mark that a session puts on a key it plans to write.

By default, intents are `'normal'`. A live intent blocks every other writer until
the session commits, rolls back, or expires.

Set `conflictPolicy: 'yield'` for background work, such as bulk rewrites and
backfills, that must never make user requests fail:

```ts
await using session = await client.beginTransaction({
  locking: 'pessimistic',
  conflictPolicy: 'yield',
});
```

- A normal writer that meets a yielding intent takes the key and does not fail.
- The yielding session then aborts at its next touch of that key, or at commit.
- Two yielding sessions conflict with each other as two normal sessions do.
- A yielding session cannot take prefix locks or range locks.
- Scripts cannot use this option.

## Commit timestamp and origin

`commitWithTimestamp` commits and returns the commit timestamp. Every write of the
transaction carries this timestamp, and so do its [watch](watches.md) events. The
timestamp is `HLC_ZERO` when the commit wrote no persistent key. HLC means hybrid
logical clock: a timestamp that combines wall-clock time with a counter.

```ts
await using session = await client.beginTransaction({ origin: 'nightly-cleanup' });
await session.delete('orders/42');
const { committed, commitTimestamp } = await session.commitWithTimestamp();
```

`origin` is a label of up to 64 characters. Every watch event of the transaction
carries it, so a watch consumer can tell background writes from user writes. The
server keeps the label only when it runs with `--transaction-origin-tags`.

A refused commit throws a `KahunaError`. Its message names the reason class, such
as `lostLock` or `decisionDeadline`, and the reason from the server.
