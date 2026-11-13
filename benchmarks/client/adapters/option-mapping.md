# Client option mapping

How each adapter maps the `WireSettings` in [`../spec.ts`](../spec.ts) to library options. This page is
the evidence for fairness rules 1, 3 and 4 in the [README](../../README.md#fairness-rules): the same wire settings for every client under
`normalized`, declared gaps instead of hidden ones, and thin adapters.

kafka-core is built from the session's git ref, so its section describes the current source. Pinned competitor
versions: `kafkajs` 2.2.4, `@platformatic/kafka` 2.15.0, `@confluentinc/kafka-javascript` 1.11.0 (KafkaJS
compatible API, librdkafka 2.16.0). Read from the installed packages and their sources on 2026-11-12.

## Rules shared by all adapters

- **Profiles.** `normalized` maps every field below. `defaults` sets only brokers, client id, group id, logging off
  and whatever is needed to read from the earliest offset; everything else is the library default.
- **Records.** `Buffer` keys, values and header values, string header keys, no serializers. kafkajs receives the
  pooled records as they are, because its `send()` only reads the messages it is given. Platformatic and Confluent
  get one library message object per record: Platformatic messages carry the topic, and Confluent's `send()` writes
  partition, timestamp and converted headers back onto the message object, which would corrupt the shared record
  pool.
- **`send()`** resolves when the library's own `send()` promise resolves, which happens after the broker
  acknowledged every record for the configured acks (librdkafka delivery reports for Confluent).
- **Logging off.** kafkajs `logLevel.NOTHING`; Confluent `kafkaJS.logLevel: NOTHING` (librdkafka `log_level` 0);
  Platformatic logs only through the `debug` module, which stays silent without `DEBUG`.
- **Consumers** subscribe from the earliest offset. The group id only arrives with `start()`, and every library
  needs it to construct a consumer, so `connect()` does nothing and `start()` creates, connects, subscribes and starts
  consumption. `start()` resolves once consumption is running, not when it ends.
- **Commits.** Under `normalized` auto-commit is off. With `commitEvery` > 0 (both profiles) auto-commit is off and,
  every `commitEvery` delivered records, the adapter commits the next offset of every partition delivered since the
  previous commit, in one request, and waits for it before delivering more (an awaited commit inside the handler; for
  the Platformatic stream, the stream is paused until the commit resolves). Every adapter, kafka-core included,
  uses the same `createOffsetCommitter` from [`shared.ts`](shared.ts). Under `defaults` with `commitEvery` 0 the
  library's auto-commit stays on, so `defaults` changes the commit cadence as well as the wire settings. Each table
  below gives the library's default cadence in its auto-commit row.
- **CRC checks.** kafka-core verifies the CRC32C of every consumed record batch by default (`checkCrcs: true`).
  kafkajs and Platformatic read the CRC field without checking it, and librdkafka's `check.crcs` defaults to false.
  Every adapter keeps its library default under both profiles. The kafkajs, Platformatic and Confluent adapters
  declare this as a gap on consume and E1 results, so the report footnotes it.

## kafka-core

| `WireSettings` field     | `normalized`                                                     | `defaults`                                                |
| ------------------------ | ---------------------------------------------------------------- | --------------------------------------------------------- |
| `acks`                   | `producer({ acks })`                                             | library default (-1)                                      |
| `lingerMs`               | `producer({ lingerMs })`                                         | library default (5)                                       |
| `batchBytes`             | `producer({ batchSize })`, a total across partitions (see below) | library default (16384)                                   |
| `compression`            | `producer({ compression: CompressionTypes.GZIP/None })`          | library default (none)                                    |
| `maxInFlight`            | `producer({ maxInFlightRequests })`                              | library default (5)                                       |
| `idempotent`             | `producer({ idempotent })`                                       | library default (false)                                   |
| `fetchMinBytes`          | `consumer({ minBytes })`                                         | library default (1)                                       |
| `fetchMaxBytes`          | `consumer({ maxBytes })`                                         | library default (10 MiB)                                  |
| `fetchMaxWaitMs`         | `consumer({ maxWaitTimeInMs })`                                  | library default (5000)                                    |
| `partitionFetchMaxBytes` | `consumer({ maxBytesPerPartition })`                             | library default (1 MiB)                                   |
| earliest offset          | `subscribe({ fromBeginning: true })`                             | same                                                      |
| auto-commit              | `run({ autoCommit: false })`                                     | library default (on, one commit after each fetched batch) |

No `WireSettings` gaps.

`batchBytes` is not like-for-like. kafka-core flushes the linger buffer once the bytes buffered across all
partitions reach `batchSize`. Java's and librdkafka's `batch.size` apply to each partition's batch. In P3 to P5,
256 single-record sends spread over 3 partitions fill 16 KiB of total bytes after about 142 records, so kafka-core
flushes without waiting out the 5 ms linger, while librdkafka's per-partition batches stay below `batch.size` and
wait for it. This favours kafka-core in those scenarios; P2 is the produce comparison without this effect.

Default auto-commit cadence: `autoCommitInterval` and `autoCommitThreshold` are unset, so kafka-core commits once
after each fetched partition batch, for `eachMessage` and `eachBatch` alike. Earlier builds, including the one session
`20261112113347-fe9c38b14aa8` ran, also committed after every 500-record `eachMessage` slice (`maxRecords`): about
4,000 extra commits per 2 million records in C1 `defaults`.

Consumer modes: `each-message` uses `run({ eachMessage })`, `each-batch` uses `run({ eachBatch })` with one
`onBatch` call per fetched partition batch. `partitionsConsumedConcurrently` stays at its default of 1.

## kafkajs 2.2.4

| `WireSettings` field     | `normalized`                                         | `defaults`                      |
| ------------------------ | ---------------------------------------------------- | ------------------------------- |
| `acks`                   | `send({ acks })` per call                            | library default (-1)            |
| `lingerMs`               | **gap**: no linger, every `send()` is one request    | n/a                             |
| `batchBytes`             | **gap**: a batch is the records of one `send()` call | n/a                             |
| `compression`            | `send({ compression: CompressionTypes.GZIP/None })`  | library default (none)          |
| `maxInFlight`            | `producer({ maxInFlightRequests })`                  | library default (unbounded)     |
| `idempotent`             | `producer({ idempotent })`                           | library default (false)         |
| `fetchMinBytes`          | `consumer({ minBytes })`                             | library default (1)             |
| `fetchMaxBytes`          | `consumer({ maxBytes })`                             | library default (10 MiB)        |
| `fetchMaxWaitMs`         | `consumer({ maxWaitTimeInMs })`                      | library default (5000)          |
| `partitionFetchMaxBytes` | `consumer({ maxBytesPerPartition })`                 | library default (1 MiB)         |
| earliest offset          | `subscribe({ fromBeginning: true })`                 | same                            |
| auto-commit              | `run({ autoCommit: false })`                         | library default (on, see below) |

Default auto-commit cadence: `autoCommitInterval` and `autoCommitThreshold` are unset, so the per-message
`commitOffsetsIfNecessary` is a no-op (`src/consumer/offsetManager/index.js:200-213`) and the runner commits once
after each fetched partition batch (`src/consumer/runner.js:457`).

Consumer modes: `each-message` uses `run({ eachMessage })`, `each-batch` uses `run({ eachBatch })` with one
`onBatch` call per fetched partition batch. `partitionsConsumedConcurrently` stays at its default of 1.

Notes: on Node 26 and Bun, kafkajs prints a `TimeoutNegativeWarning` to stderr from its own request queue; it is
harmless and does not reach the result line.

## @platformatic/kafka 2.15.0

| `WireSettings` field     | `normalized`                                                                 | `defaults`                      |
| ------------------------ | ---------------------------------------------------------------------------- | ------------------------------- |
| `acks`                   | `send({ acks })` per call (the constructor's `acks` is not read by `send()`) | library default (1)             |
| `lingerMs`               | **gap**: no linger, every `send()` is one request per partition leader       | n/a                             |
| `batchBytes`             | **gap**: a batch is the records of one `send()` call                         | n/a                             |
| `compression`            | `send({ compression: 'gzip' / 'none' })`                                     | library default (none)          |
| `maxInFlight`            | `Producer({ maxInflights })`; **gap** when idempotent: the library forces 1  | library default (5)             |
| `idempotent`             | `Producer({ idempotent })`                                                   | library default (false)         |
| `fetchMinBytes`          | `Consumer({ minBytes })`                                                     | library default (1)             |
| `fetchMaxBytes`          | `Consumer({ maxBytes })`                                                     | library default (50 MiB)        |
| `fetchMaxWaitMs`         | `Consumer({ maxWaitTime })`                                                  | library default (5000)          |
| `partitionFetchMaxBytes` | `Consumer({ maxBytesPerPartition })`                                         | library default (maxBytes)      |
| earliest offset          | `consume({ mode: MessagesStreamModes.EARLIEST })`                            | same                            |
| auto-commit              | `Consumer({ autocommit: false })`                                            | library default (on, see below) |

Default auto-commit cadence: `autocommit: true` sets no timer. After each fetch response is pushed into the consume
stream, the stream queues the next offset of every partition in it and commits them in one request, before the
application has read those records. While a commit is in flight, newly queued offsets wait for the next push
(`dist/clients/consumer/messages-stream.js`, `#pushRecords` and `[kAutocommit]`).

Header keys use the library's own `stringSerializer` for `headerKey`, because its record writer needs `Buffer` header
keys; kafkajs and librdkafka convert string header keys internally in the same way. Keys, values and header values
use the default no-op serializers. `connect()` on the producer sends an empty metadata request so the bootstrap
connection opens before the first `send()`, as `connect()` does for the other clients.

Consumer modes: `each-message` listens to the stream's `'data'` event, as Platformatic's own consumer benchmark does.
There is no batch API (declared gap): `each-batch` drains the stream on each `'readable'` event and reports what was
buffered as one batch. The library pushes all records of a fetch response synchronously, so a drain normally equals
one fetch response across all assigned partitions.

## @confluentinc/kafka-javascript 1.11.0 (KafkaJS-compatible API)

Identity and logging go in the `kafkaJS` block, as a KafkaJS user writes them. Wire settings use librdkafka
properties at the top level; the library applies them after the `kafkaJS` block, and they cover what the block cannot
express.

| `WireSettings` field     | `normalized`                                                          | `defaults`                        |
| ------------------------ | --------------------------------------------------------------------- | --------------------------------- |
| `acks`                   | `acks`                                                                | librdkafka default (-1)           |
| `lingerMs`               | `linger.ms`                                                           | librdkafka default (5)            |
| `batchBytes`             | `batch.size`                                                          | librdkafka default (1000000)      |
| `compression`            | `compression.codec`                                                   | librdkafka default (none)         |
| `maxInFlight`            | `max.in.flight.requests.per.connection`                               | librdkafka default (1000000)      |
| `idempotent`             | `enable.idempotence`                                                  | librdkafka default (false)        |
| `fetchMinBytes`          | `fetch.min.bytes`                                                     | librdkafka default (1)            |
| `fetchMaxBytes`          | `fetch.max.bytes`                                                     | librdkafka default (52428800)     |
| `fetchMaxWaitMs`         | `fetch.wait.max.ms`                                                   | librdkafka default (500)          |
| `partitionFetchMaxBytes` | `max.partition.fetch.bytes` and its alias `fetch.message.max.bytes`   | 10 MiB, see below                 |
| earliest offset          | `kafkaJS: { fromBeginning: true }` (`auto.offset.reset` = `earliest`) | same                              |
| auto-commit              | `enable.auto.commit: false`                                           | `kafkaJS` default (on, every 5 s) |

No `WireSettings` gaps.

Why both partition fetch names: with a `kafkaJS` block present, the library always writes
`max.partition.fetch.bytes` (1 MiB, or `maxBytesPerPartition`) and then `fetch.message.max.bytes` (10 MiB, or the
KafkaJS `maxBytes`). Both are names of the same librdkafka property, and the last one applied wins, so the KafkaJS
`maxBytes`/`maxBytesPerPartition` options do not mean what they mean in kafkajs. The adapter therefore sets the
librdkafka properties directly, and sets both names. Under `defaults` this quirk leaves the per-partition fetch size at
10 MiB.

With a `kafkaJS` block the library also switches to KafkaJS compatibility defaults (for example the `murmur2_random`
partitioner, `retries` 5, `socket.timeout.ms` 30000). This only matters under `defaults`, which is meant to show what
a KafkaJS user gets.

Consumer modes: `each-message` uses `run({ eachMessage })`, `each-batch` uses `run({ eachBatch })`. Batches are capped
by `js.consumer.max.batch.size`, default 32 records, which the adapter leaves at its default because it is not a wire
setting: Confluent's C2 numbers therefore come from batches of at most 32 records, while kafkajs and Platformatic
batches follow the fetch response.

The native addon has prebuilt binaries for Node 24 only at this version; on Node 26 and Bun importing the adapter
fails, which the harness records as unsupported.

## Alignment with upstream benchmark code

- **Platformatic** [`benchmarks/consumer.ts`](https://github.com/platformatic/kafka/blob/v2.15.0/benchmarks/consumer.ts):
  the Platformatic consumer is constructed with `minBytes`, `maxBytes`, `maxWaitTime` and `autocommit: false`,
  consumed with `consume({ mode: MessagesStreamModes.EARLIEST })` and read through `'data'` events, then closed with
  `close(true)`. The adapter does the same. Their kafkajs consumer uses `partitionsConsumedConcurrently: 1` (the
  default, kept here) and their Confluent KafkaJS consumer uses a `kafkaJS` block with `groupId`, `fromBeginning`
  and `autoCommit: false`, which this adapter mirrors except for the fetch sizes explained above.
- **Platformatic** [`benchmarks/producer-batch.ts`](https://github.com/platformatic/kafka/blob/v2.15.0/benchmarks/producer-batch.ts)
  and [`producer-single.ts`](https://github.com/platformatic/kafka/blob/v2.15.0/benchmarks/producer-single.ts):
  awaited `producer.send({ messages, acks })` per batch, the call shape used here. Their runs use string serializers
  and acks 0; this suite uses Buffers and the scenario's acks.
- **Confluent** [`examples/performance/performance-primitives.js`](https://github.com/confluentinc/confluent-kafka-javascript/blob/v1.11.0/examples/performance/performance-primitives.js):
  the KafkaJS API configured with librdkafka properties (`compression.codec`, `linger.ms`, `enable.auto.commit`,
  `auto.offset.reset`), many concurrent `send()` calls awaited together, and `eachMessage` consumption. The adapter
  uses the same librdkafka property names for wire settings. Its [`MIGRATION.md`](https://github.com/confluentinc/confluent-kafka-javascript/blob/v1.11.0/MIGRATION.md)
  documents that `acks` and `compression` are producer-level, that `fromBeginning` and `autoCommit` are
  consumer-level, and the `eachBatch` size cap.

## Sources

- Installed package sources and type definitions under `benchmarks/node_modules` at the pinned versions, in
  particular `@platformatic/kafka/dist/clients/{producer,consumer}`, `@confluentinc/kafka-javascript/lib/kafkajs`
  (`_producer.js`, `_consumer.js`, `_common.js`) and `kafkajs/src/producer`.
- [Platformatic benchmarks at v2.15.0](https://github.com/platformatic/kafka/tree/v2.15.0/benchmarks)
- [Confluent performance examples at v1.11.0](https://github.com/confluentinc/confluent-kafka-javascript/tree/v1.11.0/examples/performance)
- [librdkafka configuration reference](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
