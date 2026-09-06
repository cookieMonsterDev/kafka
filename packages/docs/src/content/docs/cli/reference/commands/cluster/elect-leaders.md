---
title: 'cluster elect-leaders'
description: 'Trigger a preferred or unclean leader election on one or more partitions'
order: 20
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka cluster elect-leaders
```

Trigger a preferred or unclean leader election on one or more partitions

## Flags

| Flag                                      | Description                                                      |
| ----------------------------------------- | ---------------------------------------------------------------- |
| `--brokers <string>`                      | comma-separated broker list, e.g. localhost:9092                 |
| `--election-type <string>`                | preferred or unclean                                             |
| `--topic-partition <string> (repeatable)` | a "topic:partition" to elect a leader on (repeatable)            |
| `--all-topic-partitions`                  | elect on every eligible partition                                |
| `--from-file <string>`                    | path to a kafka-leader-election.sh --path-to-json-file JSON file |
| `--timeout <number>`                      | request timeout in ms                                            |
| `--dry-run`                               | print the election target and exit without connecting            |
| `--yes`                                   | confirm the election without an interactive prompt               |
| `--force`                                 | required for --election-type unclean, which can lose data        |

## Examples

```sh
kafka cluster elect-leaders --election-type preferred --all-topic-partitions --brokers localhost:9092 --yes
kafka cluster elect-leaders --election-type unclean --topic-partition orders:0 --force --yes --brokers localhost:9092
```
