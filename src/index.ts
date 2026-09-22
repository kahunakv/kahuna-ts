/**
 * TypeScript client for Kahuna: distributed locks, a key/value store and a
 * sequencer.
 *
 * ```ts
 * import { KahunaClient } from 'kahuna-client';
 *
 * await using client = new KahunaClient({ endpoints: 'https://localhost:8082' });
 *
 * await using lock = await client.acquireLock('orders/42', { expiry: 10_000 });
 * if (lock.acquired) {
 *   await client.set('orders/42', 'shipped');
 * }
 * ```
 */

export { KahunaClient } from './client.js';
export type {
  AcquireLockOptions,
  ExecuteScriptOptions,
  RangeOptions,
  ReadOptions,
  SetOptions,
  WriteOptions,
} from './client.js';

export { KahunaLock } from './lock.js';
export { KeyValueEntry } from './entry.js';
export { TransactionScript } from './transaction-script.js';
export type { ScriptRunOptions } from './transaction-script.js';
export { TransactionSession } from './transaction-session.js';
export type {
  SessionCallOptions,
  SessionRangeOptions,
  SessionSetOptions,
  TransactionStatus,
} from './transaction-session.js';

export {
  KahunaBackupError,
  KahunaError,
  OperationAbortedError,
  isKahunaError,
  isKeyValueCode,
  isLockCode,
} from './errors.js';

export {
  HLC_ZERO,
  compareHlc,
  hlc,
  hlcEquals,
  isHlcZero,
  snapshotAt,
  type HlcTimestamp,
} from './hlc.js';

export type {
  DecisionDurability,
  Durability,
  ErrorDomain,
  KeyValueResponseCode,
  LockResponseCode,
  RangeLockMode,
  ReadValidation,
  RouteProvenance,
  RoutingDomain,
  RoutingMode,
  SequenceResponseCode,
  SetMode,
  TransactionConflictPolicy,
  TransactionLocking,
  TransactionPriority,
} from './enums.js';

export type { KahunaClientOptions, SecurityOptions } from './options.js';

export type {
  BackupGcResult,
  BackupInfo,
  BatchReadResult,
  BatchWriteResult,
  ClusterLeaveResult,
  ClusterMember,
  ClusterMembership,
  ClusterPlacement,
  DeleteManyItem,
  ExtendOutcome,
  GetManyItem,
  KeySpaceRanges,
  LockAcquireResult,
  LockInfo,
  MergeRangesResult,
  PartitionPlacement,
  PartitionReplica,
  RangeBounds,
  RangeDescriptor,
  RangeMap,
  RangePage,
  RegisterKeyRangeResult,
  RemoveKeyRangeResult,
  RestoreResult,
  RouteHint,
  RoutingMetadata,
  ScanItem,
  ScriptParameter,
  ScriptResult,
  ScriptResultValue,
  SequenceEntry,
  SequenceRange,
  SequenceUpdate,
  SetManyItem,
  SetReplicationFactorResult,
  SnapshotFloor,
  SnapshotHold,
  SplitRangeResult,
  TransactionOptions,
  ValueInput,
} from './types.js';

export { GrpcTransport } from './transport/grpc.js';
export { RestTransport } from './transport/rest.js';
export type { CallOptions, Transport } from './transport/transport.js';

export { newLockOwner } from './transport/codec.js';

export { ClientRouteResolver } from './routing/resolver.js';
export { bucketOfKey, consistentHash } from './routing/hash.js';
