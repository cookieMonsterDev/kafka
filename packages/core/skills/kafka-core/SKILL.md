---
name: kafka-core
description: >
  Use when writing or reviewing TypeScript/JavaScript code that talks to Apache Kafka through
  @cookiemonsterdev/kafka-core on Node.js or Bun: creating a Kafka client, producing, consuming,
  administering topics or groups, or choosing between the task skills listed in this file. Read
  first, before any other kafka-core skill.
metadata:
  purpose: >-
    Entry point for @cookiemonsterdev/kafka-core. States the rules every task shares (bigint
    offsets, one client per process, connect/disconnect lifecycle, error classes) and routes to
    the setup, producer, consumer, admin, and kafkajs-migration skills.
  type: core
  library: '@cookiemonsterdev/kafka-core'
  library_version: '2.5.1'
sources:
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/core/start/introduction.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/core/start/getting-started.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/core/guides/errors.md'
  - 'cookieMonsterDev/kafka:packages/core/src/index.ts'
---

# kafka-core

`@cookiemonsterdev/kafka-core` is a TypeScript Apache Kafka client that speaks the wire protocol
directly (no JVM, no native addon). It supports brokers from Kafka 0.10 onward and negotiates
protocol versions with each broker through `ApiVersions`. The API shape follows kafkajs:
`new Kafka({ brokers }).producer() / consumer() / admin()`.

## Task skills

| Need to…                                                     | Read                                               |
| ------------------------------------------------------------ | -------------------------------------------------- |
| Install, connect, configure TLS/SASL, share a config file    | `kafka-core-setup`                                 |
| Send messages, batch, compress, use transactions             | `kafka-core-producer`                              |
| Consume with a group, commit offsets, assign, share groups   | `kafka-core-consumer`                              |
| Create topics, change configs, inspect or reset groups, ACLs | `kafka-core-admin`                                 |
| Replace `kafkajs` in an existing codebase                    | `kafka-core-migrate-from-kafkajs`                  |
| Do the same from a terminal or shell script instead of code  | `kafka-cli` in `@cookiemonsterdev/kafka-cli`       |
| Fix a `kafka.config.ts` that is not found or fails to load   | `kafka-config` in `@cookiemonsterdev/kafka-config` |

## Rules for every task

- Import everything from the package root: `import { Kafka } from '@cookiemonsterdev/kafka-core'`.
  There are no deep imports. The package is ESM-only and needs Node.js 24+ or Bun 1.4+.
- Create one `Kafka` instance per process and derive producers, consumers, and admins from it.
- Call `connect()` before use and `disconnect()` when done, or use `await using`, which calls
  `disconnect()` for you.
- Offsets, watermarks, and producer ids are `bigint` (`42n`), never strings. `JSON.stringify`
  throws on `bigint`, so convert at the boundary with `offset.toString()`.
- Catch errors by class: `KafkaProtocolError` (broker error code, has `type`, `code`,
  `retriable`), `KafkaNonRetriableError` (bad arguments, missing API, auth failure: do not retry),
  `KafkaNumberOfRetriesExceeded` (the client already retried; see `cause`).
- A broker that is too old for an API throws `KafkaServerDoesNotSupportApiKey`. Check the broker
  version before reaching for a newer feature (headers and transactions need 0.11+, ZSTD needs
  2.1+, KIP-848 needs 4.0+, share groups need 4.1+).

```ts
import { Kafka, KafkaError, KafkaNonRetriableError, KafkaProtocolError } from '@cookiemonsterdev/kafka-core';

const kafka = new Kafka({ clientId: 'my-app', brokers: ['localhost:9092'] });

await using producer = kafka.producer();
await producer.connect();

try {
  await producer.send({ topic: 'events', messages: [{ key: 'user-1', value: 'hello' }] });
} catch (error) {
  if (error instanceof KafkaProtocolError) {
    console.error(error.type, error.code, error.retriable);
  } else if (error instanceof KafkaNonRetriableError) {
    throw error;
  } else if (error instanceof KafkaError && error.retriable) {
    // The client already retried this (5 attempts by default).
  }
}
```

## Common Mistakes

- **HIGH — Treating offsets as strings or numbers.** `message.offset + 1` fails to type-check and
  `Number(offset)` loses precision past 2^53. Use `message.offset + 1n`. Source: core
  migration/breaking-changes.
- **HIGH — A new `Kafka` (and new connections) per request.** Each instance opens its own broker
  connections. Create the client once at startup and reuse it.
- **MEDIUM — Retrying a `KafkaNonRetriableError` in a loop.** Auth failures and unsupported APIs
  never succeed on retry. Fix the configuration or the broker version instead. Source: core
  guides/errors.

## References

- Full API docs: https://cookiemonsterdev.github.io/kafka/docs/core/start/introduction/
- Exact signatures for the installed version: `node_modules/@cookiemonsterdev/kafka-core/dist/index.d.ts`.
