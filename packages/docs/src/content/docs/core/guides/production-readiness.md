---
title: Production readiness
description: Choose delivery guarantees, lifecycle, security, and monitoring before deployment
order: 11
section: guides
---

Use this checklist after [Your first message](../../start/getting-started/). Choose values
from your workload and broker configuration; the local demo is not a production configuration.

## Connections and shutdown

Create one `Kafka` instance per process. Connect producers and consumers during startup,
stop accepting new work during shutdown, wait for accepted work to settle, then disconnect.
Handle both SIGINT and SIGTERM. Keep the shutdown timeout long enough for normal delivery;
forced termination can interrupt in-flight sends. The quickstart contains a runnable shutdown example.

## Delivery and commits

Choose producer idempotence explicitly (`idempotent: true` when supported). Use transactions
when atomic Kafka writes and consumed offsets need one outcome. Neither setting makes an
external database write exactly-once automatically. Make external side effects idempotent.
Only advance a consumer offset after work succeeds; understand auto-commit before overriding it.
See [Producer](../producer/) and [Consumer reference](../../reference/consumer/).

## Timeouts and resource limits

Set a delivery deadline that covers batching and expected retries. Set request and buffer
limits deliberately. A failed send may have reached the broker; preserve the event's durable
identifier when deciding whether to retry. Benchmark with representative message sizes and
partition skew using [Throughput](../throughput/).

## Security and secrets

Use TLS and a supported SASL mechanism, validate certificates, and grant only required topic
and group permissions. Read credentials from your deployment's secret mechanism, rather than
committing them in config files. Test authentication failures and credential rotation.
See [Security](../security/).

## Monitoring and recovery

Monitor request latency/timeouts, producer retries, consumer lag and rebalance frequency.
Define what the application does when retries are exhausted and how failed events are recovered.
Test a broker outage, a slow downstream service, and shutdown under load before rollout.
See [Observability](../observability/) and [Troubleshooting](../troubleshooting/).

## Compatibility and rollout

Check broker feature requirements, runtime versions, partitioner compatibility, and `bigint`
serialization boundaries. Rehearse client upgrades using [Upgrade guidance](../../migration/upgrading/).
Use a separate group or local cluster for replay experiments.
