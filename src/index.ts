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
  WatchOptions,
  WriteOptions,
} from './client.js';

export { KahunaLock } from './lock.js';
export { KeyValueEntry } from './entry.js';
export { TransactionScript } from './transaction-script.js';
export type { ScriptRunOptions } from './transaction-script.js';
export { TransactionSession } from './transaction-session.js';
export {
  KahunaWatch,
  isTerminalWatchFrame,
  watchKey,
  watchPrefix,
  watchRange,
  watchScopeMatches,
} from './watch.js';
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
  CommitRefusalClass,
  DecisionDurability,
  Durability,
  ErrorDomain,
  HistoryHoldOperation,
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
  WatchEventKind,
  WatchFrameType,
  WatchLagReason,
  WatchScopeKind,
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
  HistoryHoldAnswer,
  HistoryHoldInfo,
  HistoryHoldRequest,
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
  WatchEvent,
  WatchFrame,
  WatchRequest,
  WatchRoute,
  WatchScope,
} from './types.js';

export {
  DEFAULT_RATE_LIMIT_KEY_PREFIX,
  KahunaConcurrencyLimiter,
  KahunaFixedWindowRateLimiter,
  KahunaPartitionedRateLimiter,
  KahunaRateLimiter,
  KahunaSlidingWindowRateLimiter,
  KahunaTokenBucketRateLimiter,
  MAX_SEGMENTS_PER_WINDOW,
  RateLimitLease,
  rateLimitKeyFor,
} from './rate-limiting/index.js';
export type {
  AcquireOptions,
  ConcurrencyLimiterOptions,
  FixedWindowRateLimiterOptions,
  PartitionedRateLimiterOptions,
  QueueProcessingOrder,
  RateLimiterFailureMode,
  RateLimiterOptions,
  RateLimiterStatistics,
  SlidingWindowRateLimiterOptions,
  TokenBucketRateLimiterOptions,
} from './rate-limiting/index.js';

export { GrpcTransport } from './transport/grpc.js';
export { RestTransport } from './transport/rest.js';
export type {
  CallOptions,
  Transport,
  WatchOpening,
  WatchStream,
} from './transport/transport.js';

export { newLockOwner } from './transport/codec.js';

export { ClientRouteResolver } from './routing/resolver.js';
export { bucketOfKey, consistentHash } from './routing/hash.js';
