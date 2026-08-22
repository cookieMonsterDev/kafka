---
title: Configuration
description: KafkaConfig, ProducerConfig, ConsumerConfig, and AdminConfig
order: 5
section: reference
---

Defaults that differ from the Java client are listed once on
[Compatibility](./compatibility/). Source:
[`types/index.ts`](https://github.com/cookieMonsterDev/kafka/blob/master/packages/core/src/types/index.ts).

## `KafkaConfig`

| Field                       | Default         | Java / Apache                                                                                                                                                            |
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

| Field                    | Default                       | Java / Apache                                                                                                                               |
| ------------------------ | ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `idempotent`             | `false`                       | Java 3.0+ `enable.idempotence=true`. [enable.idempotence](https://kafka.apache.org/43/configuration/producer-configs/#enable.idempotence)   |
| `transactionalId`        |                               | [transactional.id](https://kafka.apache.org/43/configuration/producer-configs/#transactional.id)                                            |
| `transactionTimeout`     |                               | [transaction.timeout.ms](https://kafka.apache.org/43/configuration/producer-configs/#transaction.timeout.ms)                                |
| `acks`                   | `-1`                          | [acks](https://kafka.apache.org/43/configuration/producer-configs/#acks)                                                                    |
| `compression`            | none                          | [compression.type](https://kafka.apache.org/43/configuration/producer-configs/#compression.type)                                            |
| `lingerMs`               | `0`                           | Java 4.0+ `linger.ms=5`. **Next major** defaults to `5`. [linger.ms](https://kafka.apache.org/43/configuration/producer-configs/#linger.ms) |
| `batchSize`              | unset                         | **Next major** defaults to `16384`. [batch.size](https://kafka.apache.org/43/configuration/producer-configs/#batch.size)                    |
| `createPartitioner`      | murmur2                       | `Partitioners.StickyPartitioner` adds opt-in KIP-794 sticky routing (enabled by `throughputPreset()`)                                       |
| `metadataMaxAge`         | `300000`                      |                                                                                                                                             |
| `allowAutoTopicCreation` | `true`                        | [auto.create.topics.enable](https://kafka.apache.org/43/configuration/broker-configs/#auto.create.topics.enable)                            |
| `maxInFlightRequests`    | unset (`null`)                | **Next major** defaults to `5`. The preset sets `5`.                                                                                        |
| `retry`                  | 5, or unlimited if idempotent | [retries](https://kafka.apache.org/43/configuration/producer-configs/#retries)                                                              |

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

| Field                  | Default          | Java / Apache                                                                                                                                       |
| ---------------------- | ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `groupId`              | required         | [group.id](https://kafka.apache.org/43/configuration/consumer-configs/#group.id)                                                                    |
| `groupProtocol`        | `'classic'`      | [group.protocol](https://kafka.apache.org/43/configuration/consumer-configs/#group.protocol). `'consumer'` opts into KIP-848 (Kafka 4.0+)           |
| `sessionTimeout`       | `30000`          | [session.timeout.ms](https://kafka.apache.org/43/configuration/consumer-configs/#session.timeout.ms). Unused when `groupProtocol: 'consumer'`       |
| `rebalanceTimeout`     | `60000`          | [max.poll.interval.ms](https://kafka.apache.org/43/configuration/consumer-configs/#max.poll.interval.ms)                                            |
| `heartbeatInterval`    | `3000`           | [heartbeat.interval.ms](https://kafka.apache.org/43/configuration/consumer-configs/#heartbeat.interval.ms). Unused when `groupProtocol: 'consumer'` |
| `partitionAssigners`   | `[roundRobin]`   | Classic protocol only. KIP-848 uses server-side assignment                                                                                          |
| `readUncommitted`      | `false`          | Java `isolation.level=read_uncommitted`. [isolation.level](https://kafka.apache.org/43/configuration/consumer-configs/#isolation.level)             |
| `autoOffsetReset`      |                  | [auto.offset.reset](https://kafka.apache.org/43/configuration/consumer-configs/#auto.offset.reset)                                                  |
| `rackId`               | `''`             | [client.rack](https://kafka.apache.org/43/configuration/consumer-configs/#client.rack)                                                              |
| `groupInstanceId`      |                  | [group.instance.id](https://kafka.apache.org/43/configuration/consumer-configs/#group.instance.id)                                                  |
| `maxBytesPerPartition` | `1048576`        | [max.partition.fetch.bytes](https://kafka.apache.org/43/configuration/consumer-configs/#max.partition.fetch.bytes)                                  |
| `minBytes`             | `1`              | [fetch.min.bytes](https://kafka.apache.org/43/configuration/consumer-configs/#fetch.min.bytes)                                                      |
| `maxBytes`             | `10485760`       | [fetch.max.bytes](https://kafka.apache.org/43/configuration/consumer-configs/#fetch.max.bytes)                                                      |
| `maxWaitTimeInMs`      | `5000`           | [fetch.max.wait.ms](https://kafka.apache.org/43/configuration/consumer-configs/#fetch.max.wait.ms)                                                  |
| `retry`                | `{ retries: 5 }` |                                                                                                                                                     |

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
