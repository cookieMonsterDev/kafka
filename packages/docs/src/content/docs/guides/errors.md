---
title: Errors
description: Catch KafkaError, retries, and broker protocol codes
order: 4
section: guides
---

```ts
import { Kafka, KafkaError, KafkaNonRetriableError, KafkaProtocolError } from '@kafka/core';

try {
  await producer.send({ topic: 'events', messages: [{ value: 'hello' }] });
} catch (error) {
  if (error instanceof KafkaProtocolError) {
    console.error(error.type, error.code, error.retriable);
  } else if (error instanceof KafkaNonRetriableError) {
    throw error;
  } else if (error instanceof KafkaError && error.retriable) {
    // client will usually retry this already
  }
}
```

Public classes are in [`errors.ts`](https://github.com/cookieMonsterDev/kafka/blob/master/packages/core/src/errors.ts).
The catalog is [Errors](/docs/reference/errors/). Protocol codes:
[Kafka protocol](https://kafka.apache.org/43/design/protocol/).

## Retriable vs not

`KafkaError.retriable` is `true` when the operation can be retried. The client
retries those internally (default 5 attempts). `KafkaNonRetriableError` is the
base for invalid arguments, missing APIs, and auth failures — do not retry
those in a loop.

`KafkaNumberOfRetriesExceeded` means the retrier gave up. Its `cause` is the
last error.

## Protocol errors

Broker error codes surface as `KafkaProtocolError` with `type` (for example
`NOT_LEADER_OR_FOLLOWER`), numeric `code`, and `retriable`. Some codes attach
`helpUrl`. The full table is
[`protocol/error-codes.ts`](https://github.com/cookieMonsterDev/kafka/blob/master/packages/core/src/protocol/error-codes.ts).

## Capability errors

The client checks `ApiVersions` rather than a broker version string. Too-old
APIs throw `KafkaServerDoesNotSupportApiKey` (non-retriable). See
[Public API](/docs/reference/public-api/#capability-errors).
