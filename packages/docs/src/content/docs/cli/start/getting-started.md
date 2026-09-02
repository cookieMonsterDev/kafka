---
title: Getting started
description: Run your first commands against a broker
order: 3
section: start
---

Assume a broker at `localhost:9092`. Every connecting command accepts `--brokers` directly:

```sh
kafka ping --brokers localhost:9092
kafka topic create orders --brokers localhost:9092 --partitions 3 --replication-factor 1
kafka topic list --brokers localhost:9092
kafka topic describe orders --brokers localhost:9092
kafka group list --brokers localhost:9092
```

Typing `--brokers` on every call gets old fast — `kafka init` writes a `kafka.config.ts` in the
current directory, discovered automatically from then on:

```sh
kafka init
# writes ./kafka.config.ts with client.brokers = ['localhost:9092']
kafka topic list   # --brokers no longer needed
```

`kafka doctor` reports which config file (if any) was discovered, where each of its values came
from, and whether the connection actually works — the first thing to reach for when a command
connects to the wrong cluster or fails to connect at all.

```sh
kafka doctor
```

Add `--json` to any command for one machine-readable document on stdout instead of a human table:

```sh
kafka topic list --json | jq '.topics[]'
```

Next: [Configuration](../../guides/configuration/),
[Output and scripting with JSON](../../guides/output-and-scripting/),
[Command reference](../../reference/commands/).
