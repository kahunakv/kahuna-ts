import { blake3 } from '@noble/hashes/blake3.js';
import { bytesToHex } from '@noble/hashes/utils.js';

import type { TransactionPriority } from './enums.js';
import { textToBytes } from './transport/codec.js';
import type { ScriptParameter, ScriptResult } from './types.js';

/** What a script needs from the client to run. */
export interface ScriptRunner {
  executeScript(
    script: Uint8Array,
    options?: {
      hash?: string | null;
      parameters?: readonly ScriptParameter[] | null;
      priority?: TransactionPriority;
      signal?: AbortSignal;
    },
  ): Promise<ScriptResult>;
}

export interface ScriptRunOptions {
  readonly parameters?: readonly ScriptParameter[] | null;
  readonly priority?: TransactionPriority;
  readonly signal?: AbortSignal;
}

/**
 * A parsed-once transaction script.
 *
 * The BLAKE3 digest travels with every run and keys the server's parsed-script
 * cache, so a script that runs often is parsed once. The digest is of the exact
 * bytes sent, so a change to the text produces a different key rather than a stale
 * hit.
 */
export class TransactionScript {
  readonly bytes: Uint8Array;

  readonly hash: string;

  constructor(
    private readonly runner: ScriptRunner,
    script: string | Uint8Array,
  ) {
    this.bytes = typeof script === 'string' ? textToBytes(script) : script;
    this.hash = bytesToHex(blake3(this.bytes));
  }

  run(options?: ScriptRunOptions): Promise<ScriptResult> {
    return this.runner.executeScript(this.bytes, {
      hash: this.hash,
      parameters: options?.parameters ?? null,
      priority: options?.priority,
      signal: options?.signal,
    });
  }
}
