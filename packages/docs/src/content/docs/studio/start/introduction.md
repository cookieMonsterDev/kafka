---
title: Introduction
description: A local web UI for inspecting and driving an Apache Kafka cluster
order: 1
section: start
---

`@cookiemonsterdev/kafka-studio` is a local web UI for inspecting and interacting with an Apache
Kafka cluster — browse and manage topics, tail and produce messages, inspect consumer groups, and
watch a live view of traffic moving through the cluster. Launched from the command line and served
on `localhost`. It is built on
[`@cookiemonsterdev/kafka-core`](../../../core/start/introduction/) and
[`@cookiemonsterdev/kafka-config`](../../../config/start/introduction/) — no separate protocol
implementation, no separate config format.

**Status:** early. The CLI, HTTP server, and web shell run end to end: the studio can browse,
create, and configure topics; produce messages (single sends and rate-limited bursts); browse and
tail live messages; inspect consumer groups (members, per-partition lag, offset reset, deletion)
and share groups; render a live topology board of cluster activity; and browse read-only ACL,
client quota, and transaction state. Every session is authenticated, and `--read-only` is enforced
by the server, not just hidden in the UI — see [Security](../../reference/api/#security).

## Two ways to launch it

- **`kafka studio`** — a command in [`@cookiemonsterdev/kafka-cli`](../../../cli/start/introduction/),
  resolved at runtime: install the studio alongside the CLI and `kafka studio` launches it, with an
  install hint if the studio package isn't present.
- **`kafka-studio`** — the studio's own standalone binary, usable without the CLI at all.

Both start the same server; see [Installation](../installation/) for both paths, and
[Running it against a broker](../../guides/local-development/) for pointing it at a real cluster —
including a one-command Docker broker for trying it locally.

Next: [Installation](../installation/), [Running it against a broker](../../guides/local-development/),
[API reference](../../reference/api/).
