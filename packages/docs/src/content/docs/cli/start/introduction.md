---
title: Introduction
description: A command-line admin client for Apache Kafka
order: 1
section: start
---

`@cookiemonsterdev/kafka-cli` is a command-line admin client for Apache Kafka, built on
[`@cookiemonsterdev/kafka-core`](../../../core/start/introduction/): `kafka topic list`,
`kafka group describe`, `kafka acl add`, and the rest of the broker admin surface, runnable with
`npx`/`pnpm dlx` or installed globally.

It covers **admin operations only** for the 1.x line — creating and inspecting topics, consumer
groups, ACLs, configs, quotas, transactions, and cluster state. It does not produce or consume
messages (`kafka-console-producer.sh`/`kafka-console-consumer.sh` have no equivalent here).

Every command that connects reads its brokers from an explicit `--brokers` flag, a `kafka.config.*`
file, or a named profile — see [Configuration](../../guides/configuration/). Output is human-
readable by default, or one JSON document on stdout with `--json` — see
[Output and scripting with JSON](../../guides/output-and-scripting/).

This site documents the command surface. The package lives in
[`packages/cli`](https://github.com/cookieMonsterDev/kafka/tree/develop/packages/cli); this Astro
site is `packages/docs`.

Next: [Installation](../installation/), [Getting started](../getting-started/),
[Command reference](../../reference/commands/), [Exit codes](../../reference/exit-codes/).

## Coming from the shell scripts

Already know `kafka-topics.sh`, `kafka-consumer-groups.sh`, or the rest of the scripts shipped in
an Apache Kafka distribution? See
[Migrating from the Kafka shell scripts](https://github.com/cookieMonsterDev/kafka/blob/develop/packages/cli/docs/migrating-from-shell-scripts.md)
for a flag-by-flag map to the equivalent `kafka` command.
