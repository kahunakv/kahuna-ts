# Cluster and operations

These calls manage the cluster itself. Most applications do not need them.

```ts
// Membership and replication
await client.getClusterMembership();
await client.getClusterPlacement();
await client.setReplicationFactor(partitionId, 3);
await client.leaveCluster({ nodeUrl: 'https://node3:8086' });

// Key ranges: how the key space divides into partitions
await client.registerKeyRange('orders');
await client.getRanges();
await client.splitRange('orders', 'orders/50000');
await client.mergeRanges();

// Snapshot holds: keep old revisions readable for a reader such as a report
await client.acquireSnapshotHold('reporting-job', floor, 60_000);
await client.renewSnapshotHold(holdId, 60_000);
await client.releaseSnapshotHold(holdId);
await client.getSnapshotFloor();

// Backups
await client.takeFullBackup();
await client.takeIncrementalBackup(parentBackupId);
await client.listBackups();
await client.restore(leafBackupId, '/var/lib/kahuna/restored');
await client.collectBackupGarbage({ dryRun: true });
```

`splitRange` returns the status `PartitionLimitReached` when the cluster is at its
partition limit, or the key space is at its range quota. The call changes nothing.
A retry gets the same answer until a removal or a merge frees a slot.

## Admin key

A node started with an admin key file refuses the admin calls without that key.
The admin calls are:

- `leaveCluster`, `setReplicationFactor`
- `registerKeyRange`, `removeKeyRange`, `splitRange`, `mergeRanges`
- `takeFullBackup`, `takeIncrementalBackup`, `takeCoordinatedBackup`, `restore`, `collectBackupGarbage`

Set `adminApiKey` to send the key:

```ts
const admin = new KahunaClient({ endpoints, adminApiKey: process.env.KAHUNA_ADMIN_KEY });
```

The client sends the key with the admin calls only:

| Transport | Carrier |
| --- | --- |
| REST | `X-Kahuna-Admin-Key` header |
| gRPC | `x-kahuna-admin-key` metadata entry |

When a node refuses an admin call, the client throws a `KahunaError` whose message
names `adminApiKey`.
