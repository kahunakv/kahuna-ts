import type {
  Durability,
  KeyValueResponseCode,
  RangeLockMode,
  RouteProvenance,
  SequenceResponseCode,
  TransactionPriority,
} from './enums.js';
import type { HlcTimestamp } from './hlc.js';

/** A value the caller may hand to a write. `null` means "the key holds no value". */
export type ValueInput = string | Uint8Array | null;

/**
 * Advisory routing hint a response carries.
 *
 * A hint never changes an outcome. The node that receives a request resolves the
 * resource itself, so a stale hint costs one inter-node forward and nothing else.
 */
export interface RouteHint {
  readonly partitionId: number;
  readonly endpoint: string;
  readonly provenance: RouteProvenance;
  readonly generation: number;
}

/** What a lock currently holds, as the server reports it. */
export interface LockInfo {
  readonly owner: Uint8Array | null;
  readonly expires: HlcTimestamp;
  readonly fencingToken: number;
}

/** Result of an attempt to take a lock. */
export type LockAcquireResult = 'acquired' | 'conflicted' | 'error';

/** One row of a bucket or range scan. */
export interface ScanItem {
  readonly key: string;
  readonly value: Uint8Array | null;
  readonly revision: number;
  readonly lastModified: HlcTimestamp;
}

/** One page of a range scan. */
export interface RangePage {
  readonly items: ScanItem[];
  readonly nextCursor: string | null;
  readonly hasMore: boolean;
}

/** One entry of a batched write. */
export interface SetManyItem {
  readonly key: string;
  readonly value?: ValueInput;
  readonly compareValue?: ValueInput;
  readonly compareRevision?: number;
  readonly expiry?: number;
  readonly flags?: number;
  readonly durability?: Durability;
}

/** One entry of a batched delete. */
export interface DeleteManyItem {
  readonly key: string;
  readonly durability?: Durability;
}

/** One entry of a batched read. */
export interface GetManyItem {
  readonly key: string;
  readonly revision?: number;
  readonly durability?: Durability;
}

/** The server's answer for one key of a batched write or delete. */
export interface BatchWriteResult {
  readonly key: string;
  readonly type: KeyValueResponseCode;
  readonly revision: number;
  readonly lastModified: HlcTimestamp;
  readonly durability: Durability;
  readonly routeIndex: number;
}

/** The server's answer for one key of a batched read. */
export interface BatchReadResult extends BatchWriteResult {
  readonly value: Uint8Array | null;
}

/** A named parameter substituted into a transaction script. */
export interface ScriptParameter {
  readonly key: string;
  readonly value: string | null;
}

/** One value a transaction script produced. */
export interface ScriptResultValue {
  readonly key: string;
  readonly value: Uint8Array | null;
  readonly revision: number;
  readonly expires: HlcTimestamp;
  readonly lastModified: HlcTimestamp;
}

/** What running a transaction script produced. */
export interface ScriptResult {
  readonly type: KeyValueResponseCode;
  readonly servedFrom: string | null;
  readonly values: ScriptResultValue[];
  readonly timeElapsedMs: number;
}

/** The identity and lifetime settings of a transaction session. */
export interface TransactionOptions {
  /** Milliseconds the transaction may live once started. */
  timeout?: number;
  /**
   * Milliseconds this caller will queue for an admission slot when the server is at
   * its session ceiling. Zero asks for the server default. Kept apart from
   * {@link TransactionOptions.timeout}: a long-running transaction is not thereby
   * willing to wait a long time to begin.
   */
  admissionWaitMs?: number;
  locking?: import('./enums.js').TransactionLocking;
  asyncRelease?: boolean;
  autoCommit?: boolean;
  readValidation?: import('./enums.js').ReadValidation;
  decisionDurability?: import('./enums.js').DecisionDurability;
  /** Transaction-wide snapshot for reads. Zero reads the latest committed value. */
  readTimestamp?: HlcTimestamp;
  priority?: TransactionPriority;
  /**
   * Whether this transaction yields its point-key write intents to foreground
   * writers. Defaults to `'normal'`. Use `'yield'` for maintenance work that must
   * never make a foreground transaction fail. A yielding transaction may not hold
   * prefix or range locks, and the option applies to interactive sessions only.
   */
  conflictPolicy?: import('./enums.js').TransactionConflictPolicy;
}

/** Identity of one operation inside a transaction, for idempotent replay. */
export interface OperationId {
  readonly high: number;
  readonly low: number;
}

/** The operation id that carries no identity. */
export const NO_OPERATION_ID: OperationId = Object.freeze({ high: 0, low: 0 });

/**
 * Mints a fresh operation id.
 *
 * Both halves stay inside the safe integer range so the id survives JSON without a
 * `BigInt`. Two 53-bit halves give 106 bits of entropy, far past what a collision
 * inside one transaction would need.
 */
export function newOperationId(): OperationId {
  return { high: randomSafeInteger(), low: randomSafeInteger() };
}

function randomSafeInteger(): number {
  return Math.floor(Math.random() * Number.MAX_SAFE_INTEGER);
}

export function isOperationIdEmpty(id: OperationId | undefined): boolean {
  return id === undefined || (id.high === 0 && id.low === 0);
}

/** Everything a transactional call needs beyond its own arguments. */
export interface TransactionContext {
  readonly transactionId: HlcTimestamp;
  readonly coordinatorKey: string;
  readonly operationId: OperationId;
}

/** A sequence as the server holds it. */
export interface SequenceEntry {
  readonly name: string;
  readonly currentValue: number;
  readonly initialValue: number;
  readonly increment: number;
  readonly maxValue: number | null;
  /** Values this sequence reserves per commit, or null when the server-wide setting applies. */
  readonly blockSize: number | null;
  /**
   * How many times an update has deliberately broken this sequence's value stream.
   * Values that carry different incarnations come from separate streams, so the
   * sequence does not guarantee that they do not overlap.
   */
  readonly incarnation: number;
  readonly revision: number;
  readonly durability: 'persistent';
  readonly createdAt: HlcTimestamp;
  readonly updatedAt: HlcTimestamp;
}

/**
 * The change `updateSequence` applies to a sequence. Every field is optional: an
 * omitted field leaves that parameter as the sequence already has it.
 *
 * `maxValue` and `blockSize` are optional on the sequence too, so omitting one
 * cannot also mean "remove it". Each has a companion `remove*` flag that does.
 * The server rejects a value together with its own `remove*` flag.
 */
export interface SequenceUpdate {
  /** New reserved high-water mark. The next value issued is this plus the increment. */
  readonly currentValue?: number;
  /** New step between values. Must be positive. */
  readonly increment?: number;
  /** New recorded starting value. Descriptive only; it does not move the counter. */
  readonly initialValue?: number;
  /** New maximum. Must not be below the current value. */
  readonly maxValue?: number;
  /** Removes the maximum. */
  readonly removeMaxValue?: boolean;
  /** New per-sequence block size. Must be at least 1; `1` is gap-free at one commit per value. */
  readonly blockSize?: number;
  /** Removes the per-sequence block size, which returns the sequence to the server-wide setting. */
  readonly removeBlockSize?: boolean;
}

/** A block of sequence values reserved for the caller. */
export interface SequenceRange {
  readonly name: string;
  readonly start: number;
  readonly end: number;
  readonly count: number;
  readonly revision: number;
}

/** Arguments shared by the range read and the range lock. */
export interface RangeBounds {
  readonly prefix: string;
  readonly startKey?: string | null;
  readonly startInclusive?: boolean;
  readonly endKey?: string | null;
  readonly endInclusive?: boolean;
}

/** Arguments of a range lock. */
export interface RangeLockRequest extends RangeBounds {
  readonly mode?: RangeLockMode;
}

// ── Cluster and administration ───────────────────────────────────────────────

export interface ClusterMember {
  readonly endpoint: string;
  readonly nodeId: number;
  readonly role: string;
  readonly joinedVersion: number;
}

export interface ClusterMembership {
  readonly membershipVersion: number;
  readonly members: ClusterMember[];
  readonly localRole: string;
  readonly initialized: boolean;
}

export interface PartitionReplica {
  readonly endpoint: string;
  readonly role: string;
}

export interface PartitionPlacement {
  readonly partitionId: number;
  readonly state: string;
  readonly generation: number;
  readonly effectiveReplicationFactor: number;
  readonly hostedLocally: boolean;
  readonly replicas: PartitionReplica[];
}

export interface ClusterPlacement {
  readonly replicationFactor: number;
  readonly rebalancerEnabled: boolean;
  readonly initialized: boolean;
  readonly localEndpoint: string;
  readonly hostedPartitionCount: number;
  readonly partitions: PartitionPlacement[];
}

export interface ClusterLeaveResult {
  readonly left: boolean;
  readonly drained: boolean;
  readonly outcome: string;
  readonly membershipVersion: number;
  readonly retryable: boolean;
  readonly reason: string;
}

export interface SetReplicationFactorResult {
  readonly success: boolean;
  readonly status: string;
  readonly generation: number;
  readonly reason: string | null;
}

export interface RangeDescriptor {
  readonly startKey: string | null;
  readonly endKey: string | null;
  readonly partitionId: number;
  readonly generation: number;
}

export interface KeySpaceRanges {
  readonly keySpace: string;
  readonly routingMode: string;
  readonly descriptors: RangeDescriptor[];
}

export interface RangeMap {
  readonly initialized: boolean;
  readonly localEndpoint: string;
  readonly keySpaces: KeySpaceRanges[];
}

export interface RegisterKeyRangeResult {
  readonly success: boolean;
  readonly status: string;
  readonly seeded: boolean;
  readonly routingMode: string;
  readonly descriptorCount: number;
  readonly reason: string | null;
}

export interface RemoveKeyRangeResult {
  readonly success: boolean;
  readonly status: string;
  readonly routingMode: string;
  readonly descriptorCount: number;
  readonly reason: string | null;
}

export interface SplitRangeResult {
  readonly success: boolean;
  readonly status: string;
  readonly determinate: boolean;
  readonly newPartitionId: number;
  readonly newGeneration: number;
  readonly leaderHint: string | null;
  readonly reason: string | null;
}

export interface MergeRangesResult {
  readonly success: boolean;
  readonly status: string;
  readonly determinate: boolean;
  readonly merges: number;
  readonly leaderHint: string | null;
  readonly reason: string | null;
}

// ── Routing metadata ─────────────────────────────────────────────────────────

export interface RoutingRange {
  readonly startKey: string | null;
  readonly endKey: string | null;
  readonly partitionId: number;
  readonly generation: number;
}

export interface RoutingKeySpace {
  readonly keySpace: string;
  readonly routingMode: string;
  readonly ranges: RoutingRange[];
}

export interface PartitionLeader {
  readonly partitionId: number;
  readonly endpoint: string;
}

export interface RoutingMetadata {
  readonly initialized: boolean;
  readonly schemaVersion: number;
  readonly hashAlgorithm: string;
  readonly prefixSeparator: string;
  readonly groupSeparator: string;
  readonly hashPoolSize: number;
  readonly hashPartitionOffset: number;
  readonly sequenceStorageKeyFormat: string;
  readonly reservedKeyPrefix: string;
  readonly localEndpoint: string;
  readonly snapshotVersion: number;
  readonly coherent: boolean;
  readonly keySpaces: RoutingKeySpace[];
  readonly leaders: PartitionLeader[];
}

// ── Snapshot holds ───────────────────────────────────────────────────────────

export interface SnapshotHold {
  readonly type: KeyValueResponseCode;
  readonly holdId: string;
  readonly leaseExpiry: HlcTimestamp;
}

export interface SnapshotFloor {
  readonly effectiveFloor: HlcTimestamp;
  readonly liveHolds: number;
}

// ── Backups ──────────────────────────────────────────────────────────────────

export interface BackupInfo {
  readonly backupId: string;
  readonly formatVersion: number;
  readonly type: string;
  readonly createdAtUtc: string;
  readonly parentBackupId: string | null;
  readonly partitionCount: number;
  readonly clusterId: string | null;
  readonly coordinatorNode: string | null;
  readonly requestedKind: string | null;
  readonly actualKind: string | null;
  readonly substitutionReason: string | null;
  readonly isInvalid: boolean;
  readonly isIncomplete: boolean;
  readonly invalidReason: string | null;
  readonly minRecoverablePhysicalMs: number | null;
  readonly maxRecoverablePhysicalMs: number | null;
}

export interface RestoreResult {
  readonly targetDir: string;
  readonly partitionsRestored: number;
  readonly entriesApplied: number;
  readonly lastAppliedPhysicalMs: number;
  readonly chain: BackupInfo[];
  readonly outcome: string;
  readonly minRecoverablePhysicalMs: number;
  readonly maxRecoverablePhysicalMs: number;
}

export interface BackupGcDeletion {
  readonly backupId: string;
  readonly type: string;
  readonly createdAtUtc: string;
  readonly bytes: number;
  readonly reason: string;
}

export interface BackupGcOrphan {
  readonly name: string;
  readonly isDirectory: boolean;
  readonly reason: string;
}

export interface BackupGcResult {
  readonly applied: boolean;
  readonly bytesReclaimed: number;
  readonly retentionDeletions: BackupGcDeletion[];
  readonly orphanReclamations: BackupGcOrphan[];
}

// ── Transport-level results ──────────────────────────────────────────────────

export interface LockAcquisition {
  readonly result: LockAcquireResult;
  readonly fencingToken: number;
  readonly servedFrom: string | null;
}

export interface WriteOutcome {
  readonly success: boolean;
  readonly revision: number;
  readonly timeElapsedMs: number;
}

export interface ReadOutcome extends WriteOutcome {
  readonly value: Uint8Array | null;
  readonly lastModified: HlcTimestamp;
}

export interface ExtendOutcome {
  readonly extended: boolean;
  readonly fencingToken: number;
}

export interface BatchOutcome<T> {
  readonly items: T[];
  readonly timeElapsedMs: number;
}

export interface SequenceOutcome {
  readonly type: SequenceResponseCode;
  readonly entry: SequenceEntry | null;
  readonly revision: number;
  readonly timeElapsedMs: number;
}

export interface SequenceAllocationOutcome {
  readonly type: SequenceResponseCode;
  readonly allocation: SequenceRange;
  readonly timeElapsedMs: number;
}

export interface TransactionStart {
  readonly url: string;
  readonly transactionId: HlcTimestamp;
}

export interface TransactionCommit {
  readonly committed: boolean;
  readonly recordAnchorKey: string | null;
}
