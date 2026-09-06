---
title: 'topic delete-records'
description: 'Delete records before a given offset, per partition'
order: 58
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka topic delete-records
```

Delete records before a given offset, per partition

## Flags

| Flag                   | Description                                                                 |
| ---------------------- | --------------------------------------------------------------------------- |
| `--brokers <string>`   | comma-separated broker list, e.g. localhost:9092                            |
| `--from-file <string>` | path to a JSON file in the kafka-delete-records.sh --offset-json-file shape |
| `--yes`                | confirm the deletion without an interactive prompt                          |

## Examples

```sh
kafka topic delete-records --from-file offsets.json --brokers localhost:9092 --yes
```
