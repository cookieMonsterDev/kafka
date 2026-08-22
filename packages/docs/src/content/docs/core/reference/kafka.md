---
title: Kafka client
description: new Kafka, producer, consumer, admin, and logger
order: 1
section: reference
---

```ts
import { Kafka, logLevel } from '@cookiemonsterdev/kafka-core';

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092'],
  logLevel: logLevel.INFO,
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'my-group' });
const admin = kafka.admin();
kafka.logger().info('ready');
```

Source: [`client.ts`](https://github.com/cookieMonsterDev/kafka/blob/master/packages/core/src/client.ts).
Config: [`KafkaConfig`](./configuration/#kafkaconfig).

One `Kafka` instance per process. Each `producer()`, `consumer()`,
`shareConsumer()`, and `admin()` call gets its own cluster connection pool.

## Methods

| Method                  | Returns         | Notes                                                     |
| ----------------------- | --------------- | --------------------------------------------------------- |
| `producer(config?)`     | `Producer`      | Optional `ProducerConfig`. [Producer](./producer/)        |
| `consumer(config)`      | `Consumer`      | `groupId` is required. [Consumer](./consumer/)            |
| `shareConsumer(config)` | `ShareConsumer` | KIP-932 share groups, Kafka 4.1+. [Consumer](./consumer/) |
| `admin(config?)`        | `Admin`         | [Admin](./admin/)                                         |
| `logger()`              | `Logger`        | Shared logger for this client                             |

`connect` / `disconnect` / `send` / `run` take an optional `{ signal?: AbortSignal }`.
Producer, consumer, share consumer, and admin implement `Symbol.asyncDispose`.
