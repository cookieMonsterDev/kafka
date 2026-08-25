---
title: Breaking changes
description: Offsets as bigint, MessageSet, built-in ZSTD, and environment variables
order: 1
section: migration
---

`@cookiemonsterdev/kafka-core` is a TypeScript Apache Kafka client for
Node.js that speaks the Kafka wire protocol directly. This page lists the
differences that break copy-paste from other Node clients. Constructor
defaults are tabulated under [Compatibility](../../reference/compatibility/).

## Offsets are `bigint`

Every offset, watermark, and producer id is `bigint` (`42n`), not a string.
`JSON.stringify` cannot serialize `bigint`; convert at the boundary
(`offset.toString()`) or keep the value as `bigint` through your app.

`consumer.seek()` still accepts `bigint | number | string` and coerces to
`bigint`. Committed offsets and fetch payloads always return `bigint`.

## MessageSet on Kafka 0.10

Kafka 0.10 brokers speak MessageSet (magic 0/1). The client encodes that format
when `ApiVersions` advertises Produce v0–v2 / Fetch v0–v3.

MessageSet has no headers, no transactions, and no idempotence. Sending headers
or enabling `idempotent` / `transactionalId` against a 0.10 broker throws a
non-retriable error. See [Public API](../../reference/public-api/#capability-errors).

Kafka 0.11+ negotiates RecordBatch (magic 2).

## ZSTD is built in

`CompressionTypes.ZSTD` uses Node’s `zlib.zstd*` APIs. There is no extra
native addon. That is why the runtime floor is **Node.js 24**.

ZSTD on the wire needs Produce v7+ / Fetch v10+ (Kafka 2.1+). An older broker
throws. GZIP, Snappy, LZ4, and ZSTD are built in; codecs remain overridable via
`CompressionCodecs`.

## Environment variables

All environment variables use a `KAFKA_*` prefix.

| Variable                                  | Effect                                                             |
| ----------------------------------------- | ------------------------------------------------------------------ |
| `KAFKA_LOG_LEVEL`                         | Overrides `logLevel` (`NOTHING`, `ERROR`, `WARN`, `INFO`, `DEBUG`) |
| `KAFKA_NO_PARTITIONER_WARNING=1`          | Silences the default-partitioner change warning                    |
| `KAFKA_DEBUG_PROTOCOL_BUFFERS=1`          | Logs request/response buffers                                      |
| `KAFKA_DEBUG_EXTENDED_PROTOCOL_BUFFERS=1` | Also logs fetch bodies (requires the previous flag)                |
| `KAFKA_VERSION`                           | Integration-test compose stack (default `4.0`)                     |
| `KAFKA_EXTERNAL=1`                        | Skip compose up/down in integration tests                          |
| `DO_NOT_STOP=1`                           | Leave the integration cluster running                              |

`KAFKA_VERSION` / `KAFKA_EXTERNAL` / `DO_NOT_STOP` are test runner flags, not
client config. See [Testing](../../guides/testing/).

## Linger, batch size, and in-flight defaults

Constructor defaults are throughput-oriented: `lingerMs: 5`, `batchSize: 16384`,
and `maxInFlightRequests: 5`. Pass `lingerMs: 0` for one Produce per `send()`.
Pass `maxInFlightRequests: null` to uncap in-flight requests.

`throughputPreset()` still adds the sticky partitioner, a 32 MiB `bufferMemory`,
and `partitionsConsumedConcurrently: 4` on `run()`:

```ts
import { throughputPreset } from '@cookiemonsterdev/kafka-core';

const { producer, consumer } = throughputPreset();
kafka.producer({ ...producer });
await kafka.consumer({ groupId }).run({
  ...consumer,
  eachBatch: async ({ batch }) => {
    for (const message of batch.messages) {
      void message;
    }
  },
});
```

See [Throughput](../../guides/throughput/) and
[Compatibility](../../reference/compatibility/).
