---
title: 'cluster reassign execute'
description: 'Execute a partition reassignment from a kafka-reassign-partitions.sh-shaped JSON file'
order: 27
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka cluster reassign execute
```

Execute a partition reassignment from a kafka-reassign-partitions.sh-shaped JSON file

## Flags

| Flag                   | Description                                                                            |
| ---------------------- | -------------------------------------------------------------------------------------- |
| `--brokers <string>`   | comma-separated broker list, e.g. localhost:9092                                       |
| `--from-file <string>` | path to a JSON file in the kafka-reassign-partitions.sh --reassignment-json-file shape |
| `--timeout <number>`   | request timeout in ms                                                                  |
| `--dry-run`            | print the planned reassignment and exit without connecting                             |
| `--yes`                | confirm the reassignment without an interactive prompt                                 |

## Examples

```sh
kafka cluster reassign execute --from-file reassignment.json --brokers localhost:9092 --yes
```
