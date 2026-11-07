# Watches

A watch streams an event for every committed change to the keys in its scope. A
watch works on persistent keys only.

| Scope | Covers |
| --- | --- |
| `watchKey(key)` | One key. |
| `watchPrefix(prefix)` | Every key that starts with the prefix. |
| `watchRange(...)` | A half-open interval of one key space: the start key is in, the end key is out. |

```ts
import { watchPrefix } from 'kahuna-client';

await using watch = await client.watchKeyValues({ scope: watchPrefix('orders/') });

for await (const frame of watch) {
  if (frame.type === 'event') console.log(frame.event!.kind, frame.event!.key);
}
```

When `watchKeyValues` returns, the watch is active on every partition of its scope.

## Duplicate events

The same event can arrive more than once. Keep the last event that you applied for
each key. Skip an event that is not newer:

```ts
import { watchEventSupersedes, type WatchEvent } from 'kahuna-client';

const applied = new Map<string, WatchEvent>();

for await (const frame of watch) {
  const event = frame.event;
  if (frame.type !== 'event' || !event) continue;
  if (!watchEventSupersedes(event, applied.get(event.key) ?? null)) continue;
  applied.set(event.key, event);
  // apply the event
}
```

Do not compare revisions only. An `extended` event changes the expiry and keeps the
revision.

## End of a watch

A watch ends with one final frame: `lagged`, `historyUnavailable` or `refused`.
Events between the last delivered event and the end can be missing.

A node limits how many watches it keeps open. A watch over the limit gets
`refused` with the code `watchLimitReached`.

## Resume after a gap

When the server runs with `--commit-admission-lag-ms`, each partition sends
`progress` frames. A progress frame carries a watermark: a timestamp that says
"you received every event before this point".

1. Read `watch.watermarks`. It holds the last watermark of each partition that your loop received.
2. Store the watermarks together with the state that you built from the events.
3. To replay what you missed, open a new watch with `resumePartitions` set to the stored watermarks.

With `autoResume: true`, the client resumes for you when a stream to a node breaks.

## History holds

A node deletes old log entries. A resume fails with `historyUnavailable` when the
entries it needs are gone.

A history hold stops a node from deleting the log of a partition after a timestamp.
Take a hold to make a later resume possible:

```ts
const { holds } = await client.acquireHistoryHolds('feed-1', watch.partitions, cursor, 60_000);
await client.renewHistoryHolds(holds.map((h) => h.holdId), 60_000, { timestamp: newCursor });
await client.releaseHistoryHolds(holds.map((h) => h.holdId));
```
