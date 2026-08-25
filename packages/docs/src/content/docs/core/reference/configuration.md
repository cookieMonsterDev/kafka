---
title: Configuration
description: KafkaConfig, ProducerConfig, ConsumerConfig, and AdminConfig
order: 5
section: reference
---

Defaults are product choices for this client; see [Compatibility](./compatibility/)
for the constructor-defaults table. Source:
[`types/index.ts`](https://github.com/cookieMonsterDev/kafka/blob/master/packages/core/src/types/index.ts).

## `KafkaConfig`

| Field                       | Default         | Notes                                                                                                                                                                    |
| --------------------------- | --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `brokers`                   | required        | Bootstrap `host:port`, or `() => string[] \| Promise<string[]>`                                                                                                          |
| `ssl`                       | off             | `true` or `tls.ConnectionOptions`. [SSL](https://kafka.apache.org/43/security/encryption-and-authentication-using-ssl/)                                                  |
| `sasl`                      | off             | [SASL](https://kafka.apache.org/43/security/authentication-using-sasl/). SCRAM also accepts `tokenId` / `tokenHmac` for [delegation-token login](../../guides/security/) |
| `clientId`                  | `''`            | [client.id](https://kafka.apache.org/43/configuration/producer-configs/#client.id)                                                                                       |
| `connectionTimeout`         | `1000`          | Socket connect, ms                                                                                                                                                       |
| `authenticationTimeout`     |                 | SASL handshake, ms                                                                                                                                                       |
| `reauthenticationThreshold` |                 | Reauth before session expiry, ms                                                                                                                                         |
| `requestTimeout`            |                 | Per-request, ms                                                                                                                                                          |
| `enforceRequestTimeout`     | `true`          |                                                                                                                                                                          |
| `metadataRecovery`          | `'rebootstrap'` | On `REBOOTSTRAP_REQUIRED` or an exhausted broker set, drop discovered metadata and reconnect to the original bootstrap list. `'none'` keeps retrying known brokers.      |
| `retry`                     | see below       |                                                                                                                                                                          |
| `logLevel`                  | `logLevel.INFO` | Override with `KAFKA_LOG_LEVEL`                                                                                                                                          |
| `logCreator`                | console         | Custom sink                                                                                                                                                              |

Retry defaults (`packages/core/src/retry/defaults.ts`): `retries: 5`,
`initialRetryTime: 300`, `maxRetryTime: 30000`, `multiplier: 2`, `factor: 0.2`.

`sasl.mechanism` is `plain`, `scram-sha-256`, `scram-sha-512`, `oauthbearer`,
or `gssapi`. GSSAPI fields: `serviceName` (default `kafka`), optional
`principal`, `keytab`, `krb5`, `authorizationIdentity`, and `gssProvider`.
See [Security](../../guides/security/).

## `ProducerConfig`

| Field                    | Default                       | Notes                                                                                                                                                                                                                                         |
| ------------------------ | ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `idempotent`             | `false`                       | Explicit opt-in. [enable.idempotence](https://kafka.apache.org/43/configuration/producer-configs/#enable.idempotence)                                                                                                                         |
| `transactionalId`        |                               | [transactional.id](https://kafka.apache.org/43/configuration/producer-configs/#transactional.id)                                                                                                                                              |
| `transactionTimeout`     |                               | [transaction.timeout.ms](https://kafka.apache.org/43/configuration/producer-configs/#transaction.timeout.ms)                                                                                                                                  |
| `acks`                   | `-1`                          | [acks](https://kafka.apache.org/43/configuration/producer-configs/#acks)                                                                                                                                                                      |
| `compression`            | none                          | [compression.type](https://kafka.apache.org/43/configuration/producer-configs/#compression.type)                                                                                                                                              |
| `compressionLevel`       | codec default                 | Honored by GZIP (zlib `level`, `0`-`9`) and ZSTD (`zlib.constants.ZSTD_c_compressionLevel`, roughly `1`-`22`). No-op for Snappy and LZ4 — see [Throughput](../../guides/throughput/#compression-level)                                        |
| `lingerMs`               | `0`                           | Latency-first: one Produce per `send()`. **Next major** defaults to `5`. [linger.ms](https://kafka.apache.org/43/configuration/producer-configs/#linger.ms)                                                                                   |
| `batchSize`              | unset                         | **Next major** defaults to `16384`. [batch.size](https://kafka.apache.org/43/configuration/producer-configs/#batch.size)                                                                                                                      |
| `createPartitioner`      | murmur2                       | `Partitioners.StickyPartitioner` adds opt-in KIP-794 sticky routing (enabled by `throughputPreset()`)                                                                                                                                         |
| `metadataMaxAge`         | `300000`                      |                                                                                                                                                                                                                                               |
| `allowAutoTopicCreation` | `true`                        | [auto.create.topics.enable](https://kafka.apache.org/43/configuration/broker-configs/#auto.create.topics.enable)                                                                                                                              |
| `maxInFlightRequests`    | unset (`null`)                | **Next major** defaults to `5`. The preset sets `5`.                                                                                                                                                                                          |
| `bufferMemory`           | unset (unlimited)             | Soft cap on linger-buffered bytes. The preset sets 32 MiB. [buffer.memory](https://kafka.apache.org/43/configuration/producer-configs/#buffer.memory)                                                                                         |
| `retry`                  | 5, or unlimited if idempotent | [retries](https://kafka.apache.org/43/configuration/producer-configs/#retries)                                                                                                                                                                |
| `deliveryTimeoutMs`      | `120000`                      | End-to-end deadline for one `send`/`sendBatch` call — `lingerMs`, any `bufferMemory` wait, and every retry, together. `0` disables it. [delivery.timeout.ms](https://kafka.apache.org/43/configuration/producer-configs/#delivery.timeout.ms) |
| `maxRequestSize`         | `1048576`                     | Cap, in bytes, on the uncompressed records of one Produce request. Enforced client-side before the broker ever sees the request. [max.request.size](https://kafka.apache.org/43/configuration/producer-configs/#max.request.size)             |
| `hooks`                  | unset                         | Ordered async `onSend`/`onAck` hooks (not an interceptor SPI). See [Producer hooks](../../guides/producer/#hooks)                                                                                                                             |

## `throughputPreset()`

Named load profile. Does not change constructor defaults. Call it and spread:

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

| Fragment   | Fields                                                                                          |
| ---------- | ----------------------------------------------------------------------------------------------- |
| `producer` | `lingerMs: 5`, `batchSize: 16384`, `maxInFlightRequests: 5`, sticky partitioner, `bufferMemory` |
| `consumer` | `partitionsConsumedConcurrently: 4` (a `run()` option)                                          |

See [Throughput](../../guides/throughput/) and [Compatibility](./compatibility/).

## `ConsumerConfig`

| Field                  | Default                    | Notes                                                                                                                                                                                                                |
| ---------------------- | -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `groupId`              | required for `subscribe()` | [group.id](https://kafka.apache.org/43/configuration/consumer-configs/#group.id). Optional for `assign()` (see [Assign mode](../../guides/consumer/#assign-mode)); needed there only to call `commitOffsets`         |
| `groupProtocol`        | `'classic'`                | [group.protocol](https://kafka.apache.org/43/configuration/consumer-configs/#group.protocol). `'consumer'` opts into KIP-848 (Kafka 4.0+)                                                                            |
| `sessionTimeout`       | `30000`                    | [session.timeout.ms](https://kafka.apache.org/43/configuration/consumer-configs/#session.timeout.ms). Unused when `groupProtocol: 'consumer'`                                                                        |
| `rebalanceTimeout`     | `60000`                    | [max.poll.interval.ms](https://kafka.apache.org/43/configuration/consumer-configs/#max.poll.interval.ms)                                                                                                             |
| `heartbeatInterval`    | `3000`                     | [heartbeat.interval.ms](https://kafka.apache.org/43/configuration/consumer-configs/#heartbeat.interval.ms). Unused when `groupProtocol: 'consumer'`                                                                  |
| `partitionAssigners`   | `[roundRobin]`             | Classic protocol only. KIP-848 uses server-side assignment                                                                                                                                                           |
| `readUncommitted`      | `false`                    | Isolation defaults to `read_committed`. [isolation.level](https://kafka.apache.org/43/configuration/consumer-configs/#isolation.level)                                                                               |
| `autoOffsetReset`      |                            | [auto.offset.reset](https://kafka.apache.org/43/configuration/consumer-configs/#auto.offset.reset)                                                                                                                   |
| `rackId`               | `''`                       | [client.rack](https://kafka.apache.org/43/configuration/consumer-configs/#client.rack)                                                                                                                               |
| `groupInstanceId`      |                            | [group.instance.id](https://kafka.apache.org/43/configuration/consumer-configs/#group.instance.id)                                                                                                                   |
| `maxBytesPerPartition` | `1048576`                  | [max.partition.fetch.bytes](https://kafka.apache.org/43/configuration/consumer-configs/#max.partition.fetch.bytes)                                                                                                   |
| `minBytes`             | `1`                        | [fetch.min.bytes](https://kafka.apache.org/43/configuration/consumer-configs/#fetch.min.bytes)                                                                                                                       |
| `maxBytes`             | `10485760`                 | [fetch.max.bytes](https://kafka.apache.org/43/configuration/consumer-configs/#fetch.max.bytes)                                                                                                                       |
| `maxWaitTimeInMs`      | `5000`                     | [fetch.max.wait.ms](https://kafka.apache.org/43/configuration/consumer-configs/#fetch.max.wait.ms)                                                                                                                   |
| `checkCrcs`            | `true`                     | Verify each fetched batch's CRC. `false` skips the check for throughput — see [Throughput](../../guides/throughput/#checkcrcs). [check.crcs](https://kafka.apache.org/43/configuration/consumer-configs/#check.crcs) |
| `retry`                | `{ retries: 5 }`           |                                                                                                                                                                                                                      |
| `hooks`                | unset                      | Ordered async `onConsume`/`onCommit` hooks (not an interceptor SPI). See [Consumer hooks](../../guides/consumer/#hooks)                                                                                              |

`committed`, `position`, and `currentLag` on `Consumer` read committed
offsets, fetch position, and lag without any extra config. See
[Consumer](../../guides/consumer/#committed-offsets-position-and-lag) and
[`committed` / `position` / `currentLag`](./consumer/#committed--position--currentlag).

## `ShareConsumerConfig`

| Field               | Default          | Notes                                                                              |
| ------------------- | ---------------- | ---------------------------------------------------------------------------------- |
| `groupId`           | required         | Share group id (KIP-932)                                                           |
| `heartbeatInterval` | `3000`           | Membership heartbeat interval, ms. The broker may override via ShareGroupHeartbeat |
| `maxWaitTimeInMs`   | `5000`           | ShareFetch max wait, ms                                                            |
| `minBytes`          | `1`              | ShareFetch min bytes                                                               |
| `maxBytes`          | `50MiB`          | ShareFetch max bytes                                                               |
| `maxRecords`        | `500`            | ShareFetch max records                                                             |
| `batchSize`         | `100`            | ShareFetch batch size                                                              |
| `shareAcquireMode`  | `0`              | ShareFetch v2 (Kafka 4.2+): `0` batch-optimized, `1` record-limit (KIP-1206)       |
| `rackId`            | `''`             | Optional rack for assignment                                                       |
| `retry`             | `{ retries: 5 }` |                                                                                    |

Requires Kafka 4.1+ with share groups enabled. See [Consumer](../../guides/consumer/#share-groups-kip-932).

## `AdminConfig`

| Field   | Default                            | Notes |
| ------- | ---------------------------------- | ----- |
| `retry` | inherited from `KafkaConfig.retry` |       |

Admin [admin configs](https://kafka.apache.org/43/configuration/admin-configs/).
