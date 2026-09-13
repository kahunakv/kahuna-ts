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
  | 'admissionRefused';

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

const LOCK_RESPONSE_NAMES = invert(LOCK_RESPONSE_CODES);
const KEY_VALUE_RESPONSE_NAMES = invert(KEY_VALUE_RESPONSE_CODES);
const SEQUENCE_RESPONSE_NAMES = invert(SEQUENCE_RESPONSE_CODES);
const DURABILITY_NAMES = invert(DURABILITY_CODES);
const ROUTE_PROVENANCE_NAMES = invert(ROUTE_PROVENANCE_CODES);

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
