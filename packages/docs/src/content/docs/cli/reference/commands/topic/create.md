---
title: 'topic create'
description: 'Create one or more topics'
order: 56
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka topic create <topics...>
```

Create one or more topics

## Flags

| Flag                                         | Description                                                                                                  |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `--brokers <string>`                         | comma-separated broker list, e.g. localhost:9092                                                             |
| `-p, --partitions <number>`                  | number of partitions                                                                                         |
| `-r, --replication-factor <number>`          | replication factor                                                                                           |
| `--replica-assignment <string> (repeatable)` | explicit partition=replica,replica assignment (repeatable, exclusive with --partitions/--replication-factor) |
| `--config <key=value> (repeatable)`          | a topic config entry, key=value (repeatable)                                                                 |
| `--dry-run`                                  | validate without creating anything                                                                           |
| `--if-not-exists`                            | treat an already-existing topic as success                                                                   |
| `--fail-fast`                                | issue one batched call instead of one call per topic                                                         |

## Examples

```sh
kafka topic create orders --partitions 3 --replication-factor 1
kafka topic create orders payments --dry-run
```
