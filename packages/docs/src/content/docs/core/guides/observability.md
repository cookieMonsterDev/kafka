---
title: Observability
description: Client-side OpenTelemetry metrics from instrumentation events
order: 8
section: guides
---

Instrumentation events already fire on connect, request, fetch, and rebalance.
Optional metrics record those onto an OpenTelemetry `Meter`. Default **off** —
there is no hard dependency on `@opentelemetry/api`.

## Enable metrics

Pass a meter, or set `metrics: true` to use the global meter from the optional
peer `@opentelemetry/api`:

```ts
import { metrics } from '@opentelemetry/api';
import { Kafka } from '@cookiemonsterdev/kafka-core';

const kafka = new Kafka({
  brokers: ['localhost:9092'],
  metrics: { meter: metrics.getMeter('my-app') },
});
```

`metrics: true` throws a non-retriable error if `@opentelemetry/api` is not
installed. Any object that implements the `Meter` subset (`createCounter`,
`createHistogram`, `createUpDownCounter`) works — you do not have to use
OpenTelemetry.

## What is recorded

| Metric                            | Kind            | Source                                               |
| --------------------------------- | --------------- | ---------------------------------------------------- |
| `kafka.client.connection.count`   | up-down counter | producer / consumer / admin `connect` / `disconnect` |
| `kafka.client.request.duration`   | histogram (ms)  | `network.request`                                    |
| `kafka.client.request.size`       | histogram (By)  | `network.request`                                    |
| `kafka.client.request.timeout`    | counter         | `network.request_timeout`                            |
| `kafka.client.request.queue_size` | histogram       | `network.request_queue_size`                         |
| `kafka.producer.record.send`      | counter         | successful Produce                                   |
| `kafka.producer.record.size`      | histogram (By)  | uncompressed key+value per record                    |
| `kafka.producer.batch.size`       | histogram       | records in one `send` / `sendBatch`                  |
| `kafka.producer.retry`            | counter         | Produce attempts after the first                     |
| `kafka.consumer.fetch.records`    | counter         | `end_batch_process`                                  |
| `kafka.consumer.fetch.duration`   | histogram (ms)  | Fetch / ShareFetch                                   |
| `kafka.consumer.lag`              | histogram       | high-watermark lag on a fetched batch                |
| `kafka.consumer.rebalance`        | counter         | `rebalancing`                                        |
| `kafka.consumer.group_join`       | counter         | `group_join`                                         |

Request metrics include `client`, `api_name`, and `broker` attributes. Names
are exported as `METRIC_NAMES` for KIP-714 telemetry later.

Config: [`KafkaConfig.metrics`](../../reference/configuration/#kafkaconfig).
Events: [`on()`](../../reference/kafka/).
