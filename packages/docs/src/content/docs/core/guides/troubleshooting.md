---
title: Troubleshooting
description: Diagnose connection failures, missing records, duplicates, and unstable consumers
order: 10
section: guides
---

Start with the smallest failing operation. Confirm the cluster and profile before
changing offsets or configuration. The [Errors guide](../errors/) explains error classes.

## Cannot connect to a broker

1. Run `kafka doctor --brokers localhost:9092`, then `kafka ping --brokers localhost:9092` (install the [CLI](../../../cli/start/installation/) first).
2. Check DNS and TCP reachability from the application's host or container. `localhost` inside a container means that container.
3. Check advertised broker addresses: reaching the bootstrap broker does not prove every advertised address is reachable.
4. Match TLS, CA trust, SASL mechanism, and credentials to the listener. Use [Security](../security/) for configuration.

Expected result: doctor identifies the intended configuration; ping confirms that a broker is reachable.
Doctor does not connect to brokers and cannot verify credentials or network access.
Do not repeatedly retry authentication failures; correct the configuration.

## Consumer receives no messages

1. Confirm the exact topic and cluster with `kafka topic describe <topic>`.
2. Check `kafka group offsets <group>`: an existing commit takes precedence over `fromBeginning`.
3. Check whether another group member owns the partition, and whether the consumer is paused.
4. Check transaction state: `read_committed` hides aborted records and can wait behind open transactions.
5. For a local replay, use a new group with `fromBeginning: true`; resetting a shared group's offsets changes its future delivery.

Expected result: records appear for the assigned partition. See [Offsets](../consumer-offsets/).

## Messages are processed twice

At-least-once processing can repeat work if the process crashes after a side effect but
before an offset commit. Use a durable deduplication key or an idempotent destination.
Producer idempotence addresses producer retries, not arbitrary application side effects.
See [Task recipes](../recipes/#handle-a-failed-message).

## Consumer keeps rebalancing

Check membership churn, handler latency, blocked event loops, and broker/network errors.
Correlate group events with request timeouts using [Observability](../observability/).
For long batch work, follow the heartbeat and commit contract in the [Consumer reference](../../reference/consumer/).
Changing assigners or protocol requires a coordinated rollout; see [Groups and rebalances](../consumer-rebalances/).

## Lag grows continuously

Confirm that incoming traffic exceeds processing capacity before increasing concurrency.
Inspect lag per partition: one hot key can concentrate work on one partition. Check
handler latency, downstream calls, fetch sizes, and commit frequency. Compare [Throughput](../throughput/)
settings with the ordering your application needs.

## Send times out or fails

Inspect the error class, its cause, and broker error code. A delivery timeout does not
prove that the broker rejected the record. An application retry may duplicate it.
See [Delivery timeout and retries](../producer/#delivery-timeout-and-retries).

## Config file is not found

Run `kafka doctor` from the application's working directory. Discovery stops at workspace
boundaries and refuses untrusted directories. Use `--config-file` for an explicit path.
See [Configuration](../../../cli/guides/configuration/) and [Config getting started](../../../config/start/getting-started/).
