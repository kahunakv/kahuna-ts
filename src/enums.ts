/**
 * Wire vocabulary of the Kahuna protocol, expressed as string-literal unions.
 *
 * Both transports carry these values as integers. The tables below are the single
 * place where a name becomes a number, so a change to the server contract touches
 * one file.
 */

/** How long a lock or an entry survives a node restart. */
export type Durability = 'ephemeral' | 'persistent';

/** Outcome code of a lock operation. */
export type LockResponseCode =
  | 'locked'
  | 'busy'
  | 'extended'
  | 'unlocked'
  | 'got'
  | 'waitingForReplication'
  | 'errored'
  | 'invalidInput'
  | 'mustRetry'
  | 'lockDoesNotExist'
  | 'invalidOwner'
  | 'aborted';

/** Outcome code of a key/value operation. */
export type KeyValueResponseCode =
  | 'set'
  | 'notSet'
  | 'extended'
  | 'get'
  | 'deleted'
  | 'locked'
  | 'unlocked'
  | 'prepared'
  | 'committed'
  | 'rolledBack'
  | 'exists'
  | 'waitingForReplication'
  | 'errored'
  | 'invalidInput'
  | 'mustRetry'
  | 'aborted'
  | 'doesNotExist'
  | 'alreadyLocked'
  | 'prefixLockUnsupportedOnRangedSpace'
  | 'rangeLocks'
  | 'safeTimestamp'
  | 'admissionRefused'
  | 'watchUnsupportedOnEphemeral'
  | 'historyHoldLimitReached'
  | 'watchLimitReached'
  | 'snapshotHoldLimitReached'
  | 'transactionMutationLimitReached'
  | 'notApplied';

/** Outcome code of a sequence operation. */
export type SequenceResponseCode =
  | 'success'
  | 'notFound'
  | 'alreadyExists'
  | 'invalidInput'
  | 'maxValueExceeded'
  | 'mustRetry'
  | 'aborted'
  | 'error';

/** Condition a `set` must meet before the server applies it. */
export type SetMode =
  /** Always write. */
  | 'always'
  /** Write, but archive no historical revision entry. */
  | 'noRevision'
  /** Write only when the key already holds a value. */
  | 'ifExists'
  /** Write only when the key holds no value. */
  | 'ifNotExists';

/** Concurrency control a transaction session uses. */
export type TransactionLocking = 'pessimistic' | 'optimistic';

/** Whether a transaction tracks its reads for a write-skew check at commit. */
export type ReadValidation = 'none' | 'trackAndValidate';

/** How durable the coordinator decision record must be before the client hears the outcome. */
export type DecisionDurability = 'bestEffort' | 'durable';

/** Relative admission order when the server is at its session ceiling. */
export type TransactionPriority = 'background' | 'low' | 'normal' | 'high' | 'critical';

/**
 * How an interactive transaction behaves when another writer meets one of its live
 * write intents. `normal` is the default: a live intent denies every other writer
 * until it is released, prepared or expired. `yield` marks maintenance work (bulk
 * rewrites, backfills, sweeps) that must never make foreground work fail: a normal
 * transaction or a plain write that meets a yielding intent takes the key over, and
 * the yielding transaction aborts at its next touch of the key or at commit. A
 * yielding transaction may not hold prefix or range locks.
 */
export type TransactionConflictPolicy = 'normal' | 'yield';

/** Compatibility class of a range lock. */
export type RangeLockMode = 'exclusive' | 'shared' | 'writeFence';

/** Which of the three code families an error carries. */
export type ErrorDomain = 'lock' | 'keyValue' | 'sequence';

/** Where the endpoint in a routing hint comes from. */
export type RouteProvenance = 'unknown' | 'executed' | 'forwarded';

/** Which name space a resource is routed in. */
export type RoutingDomain = 'keyValue' | 'lock' | 'sequence';

/** How the client picks the node to send an operation to. */
export type RoutingMode = 'auto' | 'roundRobin' | 'learned' | 'metadata';

/**
 * Which arm of the coordinator refused a transaction commit. The response code says
 * whether the refusal is terminal (`aborted`) or retryable (`mustRetry`); this class
 * says why, so a caller can report it without reading server logs.
 */
export type CommitRefusalClass =
  /** The commit was not refused, or the node does not report a class. */
  | 'none'
  /** Another transaction's write or lock conflicts with this transaction. */
  | 'conflict'
  /** Another commit overtook a validated base of a staged write. */
  | 'stagedBaseMoved'
  /** The decision deadline passed before the decision was recorded. */
  | 'decisionDeadline'
  /** The prepare retry budget ran out while other intents held the written keys. */
  | 'prepareExhausted'
  /** A partition leader change dropped a lock the transaction held. */
  | 'lostLock'
  /** The write intent of a staged write lapsed before the commit probe. */
  | 'lostStagedIntent'
  /** A leader change lost a confirmed staging and a later write staged over it. */
  | 'stagedChainBreak'
  /** Recovery, a reaper or a rollback fence aborted the canonical record. */
  | 'presumedAbort'
  /** The transaction was rolled back explicitly. */
  | 'explicitRollback'
  /** The working set cannot be committed as shaped. */
  | 'invalidWorkingSet'
  /** The finalize threw before anything durable was proposed. */
  | 'faulted'
  /** Retryable: decided predecessors still held the written keys at the deadline. */
  | 'settlementLag'
  /** Retryable: the attempt reached no record-backed outcome. */
  | 'unresolved'
  /** Retryable after a back-off: the node was at capacity. */
  | 'admissionCapacity'
  /** The coordinator does not know the transaction. */
  | 'unknownTransaction'
  /** Retryable: the commit timestamp fell outside the commit admission window. */
  | 'commitTimestampOutsideWindow'
  /**
   * Retryable after a short back-off: a written key, or the record anchor, is in a
   * key range that a split or a merge is moving to another partition.
   */
  | 'rangeQuiesced';

/**
 * How a read at a read timestamp treats the state of the reading transaction.
 *
 * `default` is the read every caller had before the mode existed. Without a read
 * timestamp, a transactional read pins the committed state it observes and serves
 * that pin on every later read of the key. With a read timestamp, the read serves
 * the committed state at that timestamp and does not show the transaction's own
 * staged writes.
 *
 * `statementSnapshot` reads at one timestamp per statement. The transaction's own
 * staged writes are visible, and every other key answers with the committed state
 * at the timestamp. The read neither serves nor checks the pins of earlier reads,
 * and it leaves no pin. It needs a read timestamp.
 */
export type KeyValueReadMode = 'default' | 'statementSnapshot';

/** What a key/value watch covers. */
export type WatchScopeKind =
  /** One exact key. */
  | 'key'
  /** Every key that starts with a prefix. */
  | 'prefix'
  /** Every key of one key space in the half-open interval `[startKey, endKey)`. */
  | 'range';

/** The kind of frame a watch delivers. */
export type WatchFrameType =
  /** One committed mutation of a key in the scope. */
  | 'event'
  /** The progress watermark of a partition: no later event on it is at or below it. */
  | 'progress'
  /** The partition delivered every event of one transaction. */
  | 'transactionEnd'
  /** The stream lost continuity and ended. Events can be missing. */
  | 'lagged'
  /** A resume position is below the history the replica retains. The stream ended. */
  | 'historyUnavailable'
  /** The watch was refused. The stream ended. */
  | 'refused'
  /** The watch is registered. The client reads this frame and never yields it. */
  | 'opened';

/** The kind of committed mutation a watch event reports. */
export type WatchEventKind =
  /** The key was set to a new value at a new revision. */
  | 'set'
  /** The key was deleted. */
  | 'deleted'
  /** The key's expiry was refreshed. The value and the revision did not change. */
  | 'extended';

/** Why a watch stream ended with a `lagged` frame. */
export type WatchLagReason =
  | 'none'
  /** The reader fell a full capacity behind. The backlog was dropped. */
  | 'overflow'
  /** The replica replaced the partition's state from a snapshot, an import or a purge. */
  | 'partitionStateReplaced'
  /** The replica failed to apply an entry, so it cannot prove it emitted every event. */
  | 'applyDiscontinuity'
  /** The range map changed, so the partitions the scope covers changed. */
  | 'rangeMapChanged'
  /** The node is shutting down. */
  | 'shutdown'
  /** The connection ended without a terminal frame. Set by the client only. */
  | 'disconnected'
  /** An event arrived at or below a watermark the replica already sent. */
  | 'watermarkViolated';

/** What a history hold request asks the registry to do. */
export type HistoryHoldOperation = 'acquire' | 'renew' | 'release' | 'list';

// ── Wire tables ──────────────────────────────────────────────────────────────

function invert<T extends string>(table: Readonly<Record<T, number>>): Map<number, T> {
  const reverse = new Map<number, T>();
  for (const [name, code] of Object.entries(table) as [T, number][]) reverse.set(code, name);
  return reverse;
}

export const DURABILITY_CODES: Readonly<Record<Durability, number>> = {
  ephemeral: 0,
  persistent: 1,
};

export const LOCK_RESPONSE_CODES: Readonly<Record<LockResponseCode, number>> = {
  locked: 0,
  busy: 1,
  extended: 2,
  unlocked: 3,
  got: 4,
  waitingForReplication: 10,
  errored: 99,
  invalidInput: 100,
  mustRetry: 101,
  lockDoesNotExist: 102,
  invalidOwner: 103,
  aborted: 104,
};

export const KEY_VALUE_RESPONSE_CODES: Readonly<Record<KeyValueResponseCode, number>> = {
  set: 0,
  notSet: 1,
  extended: 2,
  get: 3,
  deleted: 4,
  locked: 5,
  unlocked: 6,
  prepared: 7,
  committed: 8,
  rolledBack: 9,
  exists: 10,
  waitingForReplication: 11,
  errored: 99,
  invalidInput: 100,
  mustRetry: 101,
  aborted: 102,
  doesNotExist: 103,
  alreadyLocked: 104,
  prefixLockUnsupportedOnRangedSpace: 105,
  rangeLocks: 106,
  safeTimestamp: 107,
  admissionRefused: 108,
  watchUnsupportedOnEphemeral: 109,
  historyHoldLimitReached: 110,
  watchLimitReached: 111,
  snapshotHoldLimitReached: 112,
  transactionMutationLimitReached: 113,
  notApplied: 114,
};

export const SEQUENCE_RESPONSE_CODES: Readonly<Record<SequenceResponseCode, number>> = {
  success: 0,
  notFound: 1,
  alreadyExists: 2,
  invalidInput: 3,
  maxValueExceeded: 4,
  mustRetry: 5,
  aborted: 6,
  error: 99,
};

/** Bit flags of a `set`. The server reads exactly one condition bit plus the write bit. */
export const SET_MODE_FLAGS: Readonly<Record<SetMode, number>> = {
  always: 1,
  noRevision: 2,
  ifExists: 4,
  ifNotExists: 8,
};

/** Flag the compare-and-set verbs send instead of a {@link SetMode}. */
export const SET_IF_EQUAL_TO_VALUE_FLAG = 16;

/** Flag the compare-revision-and-set verbs send instead of a {@link SetMode}. */
export const SET_IF_EQUAL_TO_REVISION_FLAG = 32;

export const TRANSACTION_LOCKING_CODES: Readonly<Record<TransactionLocking, number>> = {
  pessimistic: 0,
  optimistic: 1,
};

export const READ_VALIDATION_CODES: Readonly<Record<ReadValidation, number>> = {
  none: 0,
  trackAndValidate: 1,
};

export const DECISION_DURABILITY_CODES: Readonly<Record<DecisionDurability, number>> = {
  bestEffort: 0,
  durable: 1,
};

export const TRANSACTION_PRIORITY_CODES: Readonly<Record<TransactionPriority, number>> = {
  background: 0,
  low: 1,
  normal: 2,
  high: 3,
  critical: 4,
};

/**
 * REST codes of {@link TransactionConflictPolicy}. The gRPC enum is offset by one so
 * that zero means "unspecified"; add one when encoding for gRPC.
 */
export const TRANSACTION_CONFLICT_POLICY_CODES: Readonly<Record<TransactionConflictPolicy, number>> = {
  normal: 0,
  yield: 1,
};

export const RANGE_LOCK_MODE_CODES: Readonly<Record<RangeLockMode, number>> = {
  exclusive: 0,
  shared: 1,
  writeFence: 2,
};

export const ROUTE_PROVENANCE_CODES: Readonly<Record<RouteProvenance, number>> = {
  unknown: 0,
  executed: 1,
  forwarded: 2,
};

export const COMMIT_REFUSAL_CLASS_CODES: Readonly<Record<CommitRefusalClass, number>> = {
  none: 0,
  conflict: 1,
  stagedBaseMoved: 2,
  decisionDeadline: 3,
  prepareExhausted: 4,
  lostLock: 5,
  lostStagedIntent: 6,
  stagedChainBreak: 7,
  presumedAbort: 8,
  explicitRollback: 9,
  invalidWorkingSet: 10,
  faulted: 11,
  settlementLag: 12,
  unresolved: 13,
  admissionCapacity: 14,
  unknownTransaction: 15,
  commitTimestampOutsideWindow: 16,
  rangeQuiesced: 17,
};

export const KEY_VALUE_READ_MODE_CODES: Readonly<Record<KeyValueReadMode, number>> = {
  default: 0,
  statementSnapshot: 1,
};

export const WATCH_SCOPE_KIND_CODES: Readonly<Record<WatchScopeKind, number>> = {
  key: 0,
  prefix: 1,
  range: 2,
};

export const WATCH_FRAME_TYPE_CODES: Readonly<Record<WatchFrameType, number>> = {
  event: 0,
  progress: 1,
  transactionEnd: 2,
  lagged: 3,
  historyUnavailable: 4,
  refused: 5,
  opened: 6,
};

export const WATCH_EVENT_KIND_CODES: Readonly<Record<WatchEventKind, number>> = {
  set: 0,
  deleted: 1,
  extended: 2,
};

export const WATCH_LAG_REASON_CODES: Readonly<Record<WatchLagReason, number>> = {
  none: 0,
  overflow: 1,
  partitionStateReplaced: 2,
  applyDiscontinuity: 3,
  rangeMapChanged: 4,
  shutdown: 5,
  disconnected: 6,
  watermarkViolated: 7,
};

export const HISTORY_HOLD_OPERATION_CODES: Readonly<Record<HistoryHoldOperation, number>> = {
  acquire: 0,
  renew: 1,
  release: 2,
  list: 3,
};

const LOCK_RESPONSE_NAMES = invert(LOCK_RESPONSE_CODES);
const KEY_VALUE_RESPONSE_NAMES = invert(KEY_VALUE_RESPONSE_CODES);
const SEQUENCE_RESPONSE_NAMES = invert(SEQUENCE_RESPONSE_CODES);
const DURABILITY_NAMES = invert(DURABILITY_CODES);
const ROUTE_PROVENANCE_NAMES = invert(ROUTE_PROVENANCE_CODES);
const COMMIT_REFUSAL_CLASS_NAMES = invert(COMMIT_REFUSAL_CLASS_CODES);
const WATCH_FRAME_TYPE_NAMES = invert(WATCH_FRAME_TYPE_CODES);
const WATCH_EVENT_KIND_NAMES = invert(WATCH_EVENT_KIND_CODES);
const WATCH_LAG_REASON_NAMES = invert(WATCH_LAG_REASON_CODES);

/**
 * True when the server asked for the same request again: `mustRetry`, or
 * `waitingForReplication`, which is lag rather than a conflict and clears on its own.
 */
export function isTransientKeyValueCode(code: KeyValueResponseCode): boolean {
  return code === 'mustRetry' || code === 'waitingForReplication';
}

export function lockResponseFromCode(code: number | undefined): LockResponseCode {
  return LOCK_RESPONSE_NAMES.get(code ?? 0) ?? 'errored';
}

export function keyValueResponseFromCode(code: number | undefined): KeyValueResponseCode {
  return KEY_VALUE_RESPONSE_NAMES.get(code ?? 0) ?? 'errored';
}

export function sequenceResponseFromCode(code: number | undefined): SequenceResponseCode {
  return SEQUENCE_RESPONSE_NAMES.get(code ?? 0) ?? 'error';
}

export function durabilityFromCode(code: number | undefined): Durability {
  return DURABILITY_NAMES.get(code ?? 0) ?? 'ephemeral';
}

export function routeProvenanceFromCode(code: number | undefined): RouteProvenance {
  return ROUTE_PROVENANCE_NAMES.get(code ?? 0) ?? 'unknown';
}

/**
 * Carried as an integer on the wire so a newer node can name a class an older
 * client does not know. Such a class reads as `none`.
 */
export function commitRefusalFromCode(code: number | undefined): CommitRefusalClass {
  return COMMIT_REFUSAL_CLASS_NAMES.get(code ?? 0) ?? 'none';
}

/** An unknown frame type reads as `refused`, the way the server's own decoder reads it. */
export function watchFrameTypeFromCode(code: number | undefined): WatchFrameType {
  return WATCH_FRAME_TYPE_NAMES.get(code ?? 0) ?? 'refused';
}

export function watchEventKindFromCode(code: number | undefined): WatchEventKind {
  return WATCH_EVENT_KIND_NAMES.get(code ?? 0) ?? 'set';
}

export function watchLagReasonFromCode(code: number | undefined): WatchLagReason {
  return WATCH_LAG_REASON_NAMES.get(code ?? 0) ?? 'none';
}
