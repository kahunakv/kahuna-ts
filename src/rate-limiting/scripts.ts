import { blake3 } from '@noble/hashes/blake3.js';
import { bytesToHex } from '@noble/hashes/utils.js';

import type { Durability } from '../enums.js';
import { textToBytes } from '../transport/codec.js';

/**
 * A script sent by its bytes and its hash, so the cluster parses it once and
 * serves later runs from its plan cache.
 */
export interface RateLimiterScript {
  readonly bytes: Uint8Array;
  readonly hash: string;
}

/** One script in its ephemeral and its persistent form. */
export interface RateLimiterScriptPair {
  readonly ephemeral: RateLimiterScript;
  readonly persistent: RateLimiterScript;
}

/*
 * The scripts behind the Kahuna-backed limiters. They are the scripts of the .NET
 * client, so a TypeScript process and a .NET process that name one key share one
 * budget.
 *
 * Each decision is one script transaction: read the state, decide, write the state
 * back. A read followed by a separate write would let two callers observe the same
 * state and both be admitted.
 *
 * Every decision script answers "1:<n>" when it grants, where n is the number of
 * permits left, and "0:<ms>" when it refuses, where ms is the time until the budget
 * can cover the request. A refusal with ms of zero means the script cannot tell.
 *
 * Time comes from hlc(), the cluster's hybrid logical clock. Every node advances
 * it, and a reading taken after another reading is never below it, so a window or
 * a bucket does not move backwards when another node answers the next call.
 *
 * Script parameters always arrive as strings, so every number is read through
 * to_int.
 */

/**
 * A fixed window. The state is "<window start>:<used>". A window that is not the
 * current one counts as empty, so the budget resets on the boundary. The state
 * expires a little after its window ends: the expiry only reclaims memory, because
 * the window start in the value, not the expiry, decides which window the count
 * belongs to.
 */
const FIXED_WINDOW = `LET now = hlc()
LET window = to_int(@window_ms)
LET limit = to_int(@limit)
LET permits = to_int(@permits)
LET start = (now / window) * window
LET used = 0
LET state = EGET @key
IF state != null THEN
  LET parts = split(to_string(state), ":")
  IF to_int(parts[0]) == start THEN
    LET used = to_int(parts[1])
  END
END
LET reset = start + window - now
LET need = permits
IF need == 0 THEN
  LET need = 1
END
IF used + need > limit THEN
  RETURN concat("0:", to_string(reset))
END
IF permits == 0 THEN
  RETURN concat("1:", to_string(limit - used))
END
ESET @key concat(to_string(start), concat(":", to_string(used + permits))) EX reset + 1000
RETURN concat("1:", to_string(limit - used - permits))`;

/**
 * A sliding window cut into segments. The state is "<last segment>:<c0>:…:<cN-1>",
 * where c0 counts the oldest segment of the window that ends with the last segment.
 * On each call the counts shift left by the number of segments that passed, so the
 * permits of a segment come back when it leaves the window.
 *
 * A refusal names the time at which enough of the oldest segments leave the window
 * to cover the request. A state with a different segment count, from a limiter
 * configured differently, is read as empty.
 */
const SLIDING_WINDOW = `LET now = hlc()
LET seg = to_int(@segment_ms)
LET n = to_int(@segments)
LET limit = to_int(@limit)
LET permits = to_int(@permits)
LET cur = now / seg
LET shift = n
LET parts = null
LET state = EGET @key
IF state != null THEN
  LET parts = split(to_string(state), ":")
  IF count(parts) == n + 1 THEN
    LET shift = cur - to_int(parts[0])
    IF shift < 0 THEN
      LET shift = 0
    END
  END
END
LET total = 0
FOR j IN 0..(n - 1) DO
  IF j + shift < n THEN
    LET total = total + to_int(parts[1 + j + shift])
  END
END
LET need = permits
IF need == 0 THEN
  LET need = 1
END
IF total + need > limit THEN
  LET freed = 0
  LET wait = 0
  LET done = false
  FOR j IN 0..(n - 1) DO
    IF done == false THEN
      IF j + shift < n THEN
        LET freed = freed + to_int(parts[1 + j + shift])
      END
      IF total - freed + need <= limit THEN
        LET wait = (cur + j + 1) * seg - now
        LET done = true
      END
    END
  END
  RETURN concat("0:", to_string(wait))
END
IF permits == 0 THEN
  RETURN concat("1:", to_string(limit - total))
END
LET out = to_string(cur)
FOR j IN 0..(n - 1) DO
  LET c = 0
  IF j + shift < n THEN
    LET c = to_int(parts[1 + j + shift])
  END
  IF j == n - 1 THEN
    LET c = c + permits
  END
  LET out = concat(out, concat(":", to_string(c)))
END
ESET @key out EX n * seg + 1000
RETURN concat("1:", to_string(limit - total - permits))`;

/**
 * A token bucket. The state is "<tokens>:<last replenishment>". Each call adds the
 * tokens of every whole period since the last replenishment, up to the limit, and
 * moves the last replenishment forward by those whole periods only, so the phase of
 * the periods is kept.
 *
 * A missing state is a full bucket. The state expires when the bucket would be full
 * again, which makes an expired state and a full bucket the same thing.
 */
const TOKEN_BUCKET = `LET now = hlc()
LET limit = to_int(@limit)
LET period = to_int(@period_ms)
LET per = to_int(@per_period)
LET permits = to_int(@permits)
LET tokens = limit
LET last = now
LET state = EGET @key
IF state != null THEN
  LET parts = split(to_string(state), ":")
  LET tokens = to_int(parts[0])
  LET last = to_int(parts[1])
  IF now > last THEN
    LET periods = (now - last) / period
    IF periods > 0 THEN
      LET tokens = tokens + periods * per
      LET last = last + periods * period
    END
  END
  IF tokens > limit THEN
    LET tokens = limit
  END
END
LET need = permits
IF need == 0 THEN
  LET need = 1
END
IF tokens < need THEN
  LET wait = ((need - tokens + per - 1) / per) * period - (now - last)
  RETURN concat("0:", to_string(wait))
END
IF permits == 0 THEN
  RETURN concat("1:", to_string(tokens))
END
LET tokens = tokens - permits
LET full = ((limit - tokens + per - 1) / per) * period - (now - last)
ESET @key concat(to_string(tokens), concat(":", to_string(last))) EX full + 1000
RETURN concat("1:", to_string(tokens))`;

/**
 * Takes concurrency permits. Every held lease is its own key under one bucket,
 * holding the number of permits it took and expiring unless its holder renews it.
 * The script sums the live leases of the bucket and writes a new lease only if the
 * sum leaves room.
 *
 * The bucket read takes a lock over the whole bucket, so two acquisitions of one
 * bucket never both count the same leases. An expired lease is not counted, which
 * is how the permits of a process that died come back.
 */
const CONCURRENCY_ACQUIRE = `LET limit = to_int(@limit)
LET permits = to_int(@permits)
LET held = 0
LET leases = EGET BY BUCKET @bucket
FOR v IN leases DO
  LET held = held + to_int(v)
END
LET need = permits
IF need == 0 THEN
  LET need = 1
END
IF held + need > limit THEN
  RETURN "0:0"
END
IF permits == 0 THEN
  RETURN concat("1:", to_string(limit - held))
END
ESET @lease_key @permits EX to_int(@lease_ms)
RETURN concat("1:", to_string(limit - held - permits))`;

/**
 * Pushes out the expiry of a held lease. The write is conditional on the lease
 * still existing: a lease that already expired gave its permits back, and writing
 * it again would take them a second time on top of whoever took them since.
 */
const CONCURRENCY_RENEW = `ESET @lease_key @permits EX to_int(@lease_ms) XX`;

/** Gives the permits of a lease back. */
const CONCURRENCY_RELEASE = `EDELETE @lease_key`;

export const FIXED_WINDOW_SCRIPT = pair(FIXED_WINDOW);
export const SLIDING_WINDOW_SCRIPT = pair(SLIDING_WINDOW);
export const TOKEN_BUCKET_SCRIPT = pair(TOKEN_BUCKET);
export const CONCURRENCY_ACQUIRE_SCRIPT = pair(CONCURRENCY_ACQUIRE);
export const CONCURRENCY_RENEW_SCRIPT = pair(CONCURRENCY_RENEW);
export const CONCURRENCY_RELEASE_SCRIPT = pair(CONCURRENCY_RELEASE);

export function selectScript(durability: Durability, scripts: RateLimiterScriptPair): RateLimiterScript {
  return durability === 'ephemeral' ? scripts.ephemeral : scripts.persistent;
}

function pair(text: string): RateLimiterScriptPair {
  return { ephemeral: script(text), persistent: script(toPersistent(text)) };
}

function script(text: string): RateLimiterScript {
  const bytes = textToBytes(text);
  return { bytes, hash: bytesToHex(blake3(bytes)) };
}

/**
 * Rewrites an ephemeral script to its persistent form. The scripts are written so
 * that the ephemeral commands are the only words that start with these letters
 * followed by a space.
 */
function toPersistent(text: string): string {
  return text.replaceAll('EGET ', 'GET ').replaceAll('ESET ', 'SET ').replaceAll('EDELETE ', 'DELETE ');
}
