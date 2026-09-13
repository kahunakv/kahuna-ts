import type { Durability, RangeLockMode, TransactionPriority } from '../enums.js';
import type { HlcTimestamp } from '../hlc.js';
import type {
  BackupGcResult,
  BackupInfo,
  BatchOutcome,
  BatchReadResult,
  BatchWriteResult,
  ClusterLeaveResult,
  ClusterMembership,
  ClusterPlacement,
  DeleteManyItem,
  ExtendOutcome,
  GetManyItem,
  LockAcquisition,
  LockInfo,
  MergeRangesResult,
  RangeBounds,
  RangeLockRequest,
  RangeMap,
  RangePage,
  ReadOutcome,
  RegisterKeyRangeResult,
  RemoveKeyRangeResult,
  RestoreResult,
  RoutingMetadata,
  ScanItem,
  ScriptParameter,
  ScriptResult,
  SequenceAllocationOutcome,
  SequenceOutcome,
  SequenceUpdate,
  SetManyItem,
  SetReplicationFactorResult,
  SnapshotFloor,
  SnapshotHold,
  SplitRangeResult,
  TransactionCommit,
  TransactionContext,
  TransactionOptions,
  TransactionStart,
  WriteOutcome,
} from '../types.js';

/** Options every transport call accepts. */
export interface CallOptions {
  readonly signal?: AbortSignal;
}

/** A write that may run inside a transaction session. */
export interface TransactionalCallOptions extends CallOptions {
  readonly transaction?: TransactionContext;
}

/** A read that may run inside a transaction session and at a snapshot. */
export interface ReadCallOptions extends TransactionalCallOptions {
  readonly readTimestamp?: HlcTimestamp;
}

/** Receives the routing hints a transport reads off responses. */
export interface RouteSink {
  learn(
    domain: import('../enums.js').RoutingDomain,
    resource: string,
    hint: import('../types.js').RouteHint | null,
    requestUrl: string,
  ): void;
  learnBatch(
    domain: import('../enums.js').RoutingDomain,
    items: readonly { key: string; routeIndex: number }[],
    table: readonly import('../types.js').RouteHint[] | null,
    requestUrl: string,
  ): void;
  reportEndpointFailure(url: string): void;
}

/**
 * Everything the client needs from one wire protocol.
 *
 * The REST and gRPC transports implement it identically, so which one a client
 * uses changes efficiency and never an operation's outcome.
 */
export interface Transport {
  /** Name of this transport, for diagnostics. */
  readonly kind: 'rest' | 'grpc';

  /** Installed by the client when routing is on. Null leaves the transport unchanged. */
  routeSink: RouteSink | null;

  /** Releases every connection this transport holds. */
  close(): Promise<void>;

  // ── Locks ──────────────────────────────────────────────────────────────────

  acquireLock(
    url: string,
    resource: string,
    owner: Uint8Array,
    expiryMs: number,
    durability: Durability,
    options?: CallOptions,
  ): Promise<LockAcquisition>;

  releaseLock(
    url: string,
    resource: string,
    owner: Uint8Array,
    durability: Durability,
    options?: CallOptions,
  ): Promise<boolean>;

  extendLock(
    url: string,
    resource: string,
    owner: Uint8Array,
    expiryMs: number,
    durability: Durability,
    options?: CallOptions,
  ): Promise<ExtendOutcome>;

  getLock(
    url: string,
    resource: string,
    durability: Durability,
    options?: CallOptions,
  ): Promise<LockInfo | null>;

  // ── Key/value point operations ────────────────────────────────────────────

  set(
    url: string,
    key: string,
    value: Uint8Array | null,
    expiryMs: number,
    flags: number,
    durability: Durability,
    options?: TransactionalCallOptions,
  ): Promise<WriteOutcome>;

  compareValueAndSet(
    url: string,
    key: string,
    value: Uint8Array | null,
    compareValue: Uint8Array | null,
    expiryMs: number,
    durability: Durability,
    options?: TransactionalCallOptions,
  ): Promise<WriteOutcome>;

  compareRevisionAndSet(
    url: string,
    key: string,
    value: Uint8Array | null,
    compareRevision: number,
    expiryMs: number,
    durability: Durability,
    options?: TransactionalCallOptions,
  ): Promise<WriteOutcome>;

  get(
    url: string,
    key: string,
    revision: number,
    durability: Durability,
    options?: ReadCallOptions,
  ): Promise<ReadOutcome>;

  exists(
    url: string,
    key: string,
    revision: number,
    durability: Durability,
    options?: ReadCallOptions,
  ): Promise<WriteOutcome>;

  delete(
    url: string,
    key: string,
    durability: Durability,
    options?: TransactionalCallOptions,
  ): Promise<WriteOutcome>;

  extend(
    url: string,
    key: string,
    expiryMs: number,
    durability: Durability,
    options?: TransactionalCallOptions,
  ): Promise<WriteOutcome>;

  // ── Key/value batches ──────────────────────────────────────────────────────

  setMany(
    url: string,
    items: readonly SetManyItem[],
    options?: CallOptions,
  ): Promise<BatchOutcome<BatchWriteResult>>;

  deleteMany(
    url: string,
    items: readonly DeleteManyItem[],
    options?: TransactionalCallOptions,
  ): Promise<BatchOutcome<BatchWriteResult>>;

  getMany(
    url: string,
    items: readonly GetManyItem[],
    options?: ReadCallOptions,
  ): Promise<BatchOutcome<BatchReadResult>>;

  existsMany(
    url: string,
    items: readonly GetManyItem[],
    options?: ReadCallOptions,
  ): Promise<BatchOutcome<BatchReadResult>>;

  // ── Scans ──────────────────────────────────────────────────────────────────

  getByBucket(
    url: string,
    prefixKey: string,
    durability: Durability,
    options?: ReadCallOptions,
  ): Promise<ScanItem[]>;

  scanAllByPrefix(
    url: string,
    prefixKey: string,
    durability: Durability,
    options?: ReadCallOptions,
  ): Promise<ScanItem[]>;

  getByRange(
    url: string,
    bounds: RangeBounds,
    limit: number,
    durability: Durability,
    options?: ReadCallOptions & { cursor?: string | null },
  ): Promise<RangePage>;

  /**
   * Walks a range page by page.
   *
   * REST resumes each page from the cursor the previous one returned; gRPC reads
   * the server stream. Both yield the same rows in the same order.
   */
  scanByRange(
    url: string,
    bounds: RangeBounds,
    pageSize: number,
    durability: Durability,
    options?: ReadCallOptions,
  ): AsyncIterable<ScanItem>;

  // ── Transaction locks ──────────────────────────────────────────────────────

  acquireExclusiveLock(
    url: string,
    key: string,
    expiryMs: number,
    durability: Durability,
    options: TransactionalCallOptions,
  ): Promise<boolean>;

  acquireExclusivePrefixLock(
    url: string,
    prefixKey: string,
    expiryMs: number,
    durability: Durability,
    options: TransactionalCallOptions,
  ): Promise<boolean>;

  releaseExclusivePrefixLock(
    url: string,
    prefixKey: string,
    durability: Durability,
    options: TransactionalCallOptions,
  ): Promise<void>;

  acquireRangeLock(
    url: string,
    request: RangeLockRequest,
    expiryMs: number,
    durability: Durability,
    mode: RangeLockMode,
    options: TransactionalCallOptions,
  ): Promise<boolean>;

  releaseRangeLock(
    url: string,
    bounds: RangeBounds,
    durability: Durability,
    options: TransactionalCallOptions,
  ): Promise<void>;

  // ── Transactions ───────────────────────────────────────────────────────────

  executeScript(
    url: string,
    script: Uint8Array,
    hash: string | null,
    parameters: readonly ScriptParameter[] | null,
    priority: TransactionPriority,
    options?: CallOptions,
  ): Promise<ScriptResult>;

  startTransaction(
    url: string,
    coordinatorKey: string,
    options: TransactionOptions,
    call?: CallOptions,
  ): Promise<TransactionStart>;

  commitTransaction(
    url: string,
    coordinatorKey: string,
    transactionId: HlcTimestamp,
    recordAnchorKey: string | null,
    call?: CallOptions,
  ): Promise<TransactionCommit>;

  rollbackTransaction(
    url: string,
    coordinatorKey: string,
    transactionId: HlcTimestamp,
    recordAnchorKey: string | null,
    call?: CallOptions,
  ): Promise<boolean>;

  // ── Sequences ──────────────────────────────────────────────────────────────

  createSequence(
    url: string,
    name: string,
    initialValue: number,
    increment: number,
    maxValue: number | null,
    blockSize: number | null,
    options?: CallOptions,
  ): Promise<SequenceOutcome>;

  /**
   * Rewrites a sequence's parameters. The server withholds its answer for one block
   * lease, so this call takes seconds by design.
   */
  updateSequence(
    url: string,
    name: string,
    update: SequenceUpdate,
    options?: CallOptions,
  ): Promise<SequenceOutcome>;

  getSequence(url: string, name: string, options?: CallOptions): Promise<SequenceOutcome>;

  nextSequenceValue(
    url: string,
    name: string,
    idempotencyKey: string | null,
    options?: CallOptions,
  ): Promise<SequenceAllocationOutcome>;

  reserveSequenceRange(
    url: string,
    name: string,
    count: number,
    idempotencyKey: string | null,
    options?: CallOptions,
  ): Promise<SequenceAllocationOutcome>;

  deleteSequence(url: string, name: string, options?: CallOptions): Promise<SequenceOutcome>;

  // ── Key ranges ─────────────────────────────────────────────────────────────

  registerKeyRange(
    url: string,
    keySpace: string,
    options?: CallOptions,
  ): Promise<RegisterKeyRangeResult>;

  removeKeyRange(url: string, keySpace: string, options?: CallOptions): Promise<RemoveKeyRangeResult>;

  getRanges(url: string, keySpace: string | null, options?: CallOptions): Promise<RangeMap>;

  splitRange(
    url: string,
    keySpace: string,
    splitKey: string,
    options?: CallOptions,
  ): Promise<SplitRangeResult>;

  mergeRanges(url: string, options?: CallOptions): Promise<MergeRangesResult>;

  // ── Cluster ────────────────────────────────────────────────────────────────

  getRoutingMetadata(
    url: string,
    keySpace: string | null,
    options?: CallOptions,
  ): Promise<RoutingMetadata>;

  getClusterMembership(url: string, options?: CallOptions): Promise<ClusterMembership>;

  getClusterPlacement(url: string, options?: CallOptions): Promise<ClusterPlacement>;

  leaveCluster(url: string, options?: CallOptions): Promise<ClusterLeaveResult>;

  setReplicationFactor(
    url: string,
    partitionId: number,
    replicationFactor: number,
    options?: CallOptions,
  ): Promise<SetReplicationFactorResult>;

  // ── Snapshot holds ─────────────────────────────────────────────────────────

  acquireSnapshotHold(
    url: string,
    holderId: string,
    timestamp: HlcTimestamp,
    leaseMs: number,
    options?: CallOptions,
  ): Promise<SnapshotHold>;

  renewSnapshotHold(
    url: string,
    holdId: string,
    leaseMs: number,
    options?: CallOptions,
  ): Promise<Omit<SnapshotHold, 'holdId'>>;

  releaseSnapshotHold(
    url: string,
    holdId: string,
    options?: CallOptions,
  ): Promise<import('../enums.js').KeyValueResponseCode>;

  getSnapshotFloor(url: string, options?: CallOptions): Promise<SnapshotFloor>;

  // ── Backups ────────────────────────────────────────────────────────────────

  takeFullBackup(url: string, options?: CallOptions): Promise<BackupInfo>;

  takeIncrementalBackup(
    url: string,
    parentBackupId: string,
    options?: CallOptions,
  ): Promise<BackupInfo>;

  takeCoordinatedBackup(url: string, options?: CallOptions): Promise<BackupInfo>;

  listBackups(url: string, options?: CallOptions): Promise<BackupInfo[]>;

  getBackupChain(url: string, leafBackupId: string, options?: CallOptions): Promise<BackupInfo[]>;

  restore(
    url: string,
    leafBackupId: string,
    targetDir: string,
    targetTimeMs: number,
    options?: CallOptions,
  ): Promise<RestoreResult>;

  collectBackupGarbage(
    url: string,
    dryRun: boolean,
    options?: CallOptions,
  ): Promise<BackupGcResult>;
}
