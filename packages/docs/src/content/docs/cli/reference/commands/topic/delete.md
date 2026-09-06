---
title: 'topic delete'
description: 'Delete one or more topics'
order: 58
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka topic delete <topics...>
```

Delete one or more topics

## Flags

| Flag                 | Description                                              |
| -------------------- | -------------------------------------------------------- |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092         |
| `--if-exists`        | treat a missing topic as success                         |
| `--yes`              | confirm the deletion without an interactive prompt       |
| `--force`            | override the internal-topic and batch-size safety checks |

## Examples

```sh
kafka topic delete orders --brokers localhost:9092 --yes
kafka topic delete __consumer_offsets --brokers localhost:9092 --yes --force
```
