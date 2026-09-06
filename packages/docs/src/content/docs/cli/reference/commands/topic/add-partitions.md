---
title: 'topic add-partitions'
description: 'Raise a topic to a new total partition count'
order: 55
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka topic add-partitions <topics...>
```

Raise a topic to a new total partition count

## Flags

| Flag                 | Description                                      |
| -------------------- | ------------------------------------------------ |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092 |
| `--count <number>`   | the new total number of partitions (not a delta) |
| `--dry-run`          | validate without changing anything               |

## Examples

```sh
kafka topic add-partitions orders --count 6 --brokers localhost:9092
kafka topic add-partitions orders payments --count 6 --dry-run
```
