# Errors, cancellation and numbers

## Errors

Every protocol failure is a `KahunaError`. Check its `domain` before its `code`,
because the three code families share several names.

```ts
import { KahunaError, isKeyValueCode } from 'kahuna-client';

try {
  await client.set('', 'value');
} catch (error) {
  if (error instanceof KahunaError) {
    console.log(error.domain, error.code); // 'keyValue' 'invalidInput'
  }
}

// The node is overloaded. Wait before you retry.
if (isKeyValueCode(error, 'admissionRefused')) await backOff();
```

An answer from the server is not an error. A write that a condition rejected, and
a read of a key with no value, both return an entry with `success` false.

### Mutation limit

A [transaction session](transactions.md#sessions) can write a limited number of
keys and hold a limited number of locks. Past the limit, the operation fails with
`transactionMutationLimitReached`.

- The session stays open. You can commit what it holds, or roll it back.
- Do not retry the operation. It fails again.
- Split the work into smaller transactions.

## Cancellation

Every method takes an `AbortSignal`:

```ts
await client.get('key', { signal: AbortSignal.timeout(2_000) });
```

A call with no signal gets the `defaultOperationTimeoutMs` deadline (30 seconds by
default). An unresponsive node fails the call and does not hang it.

## Numbers

Revisions, fencing tokens and sequence values are 64-bit integers on the wire. The
client returns them as JavaScript numbers.

A JavaScript number is exact up to `Number.MAX_SAFE_INTEGER`
(9,007,199,254,740,991). A larger value loses precision. Kahuna counters do not
reach this range in practice. A sequence with a `maxValue` above it can.
