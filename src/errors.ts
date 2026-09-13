import type {
  ErrorDomain,
  KeyValueResponseCode,
  LockResponseCode,
  SequenceResponseCode,
} from './enums.js';

/**
 * Every failure the Kahuna protocol reports.
 *
 * Read {@link KahunaError.domain} before you read {@link KahunaError.code}: the
 * three code families share several names, and only the domain tells you which
 * family the code belongs to.
 */
export class KahunaError extends Error {
  override readonly name = 'KahunaError';

  readonly domain: ErrorDomain;

  readonly code: LockResponseCode | KeyValueResponseCode | SequenceResponseCode;

  constructor(
    message: string,
    domain: ErrorDomain,
    code: LockResponseCode | KeyValueResponseCode | SequenceResponseCode,
    options?: { cause?: unknown },
  ) {
    super(message, options);
    this.domain = domain;
    this.code = code;
  }

  static lock(message: string, code: LockResponseCode, options?: { cause?: unknown }): KahunaError {
    return new KahunaError(message, 'lock', code, options);
  }

  static keyValue(
    message: string,
    code: KeyValueResponseCode,
    options?: { cause?: unknown },
  ): KahunaError {
    return new KahunaError(message, 'keyValue', code, options);
  }

  static sequence(
    message: string,
    code: SequenceResponseCode,
    options?: { cause?: unknown },
  ): KahunaError {
    return new KahunaError(message, 'sequence', code, options);
  }
}

export function isKahunaError(value: unknown): value is KahunaError {
  return value instanceof KahunaError;
}

/** True when the error is a key/value failure carrying exactly this code. */
export function isKeyValueCode(value: unknown, code: KeyValueResponseCode): boolean {
  return isKahunaError(value) && value.domain === 'keyValue' && value.code === code;
}

/** True when the error is a lock failure carrying exactly this code. */
export function isLockCode(value: unknown, code: LockResponseCode): boolean {
  return isKahunaError(value) && value.domain === 'lock' && value.code === code;
}

/**
 * Raised when a backup or restore refuses. The outcome name is the server's own,
 * so an operator sees the same word in the client and in the node log.
 */
export class KahunaBackupError extends Error {
  override readonly name = 'KahunaBackupError';

  readonly outcome: string;

  constructor(outcome: string, message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.outcome = outcome;
  }
}

/** Raised when an operation is cancelled through its {@link AbortSignal}. */
export class OperationAbortedError extends Error {
  override readonly name = 'OperationAbortedError';

  constructor(message = 'Operation aborted', options?: { cause?: unknown }) {
    super(message, options);
  }
}

/** Throws {@link OperationAbortedError} when the signal is already aborted. */
export function throwIfAborted(signal: AbortSignal | undefined): void {
  if (signal?.aborted) throw new OperationAbortedError('Operation aborted', { cause: signal.reason });
}

/** True when the value is an abort of any kind, ours or the platform's. */
export function isAbortError(value: unknown): boolean {
  if (value instanceof OperationAbortedError) return true;
  if (value instanceof Error && value.name === 'AbortError') return true;
  if (value instanceof Error && value.cause !== undefined && value.cause !== value) {
    return isAbortError(value.cause);
  }
  return false;
}
