---
title: 'topic producers'
description: "Show a topic's active producer state, per partition"
order: 63
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka topic producers <topic>
```

Show a topic's active producer state, per partition

## Flags

| Flag                                | Description                                                        |
| ----------------------------------- | ------------------------------------------------------------------ |
| `--brokers <string>`                | comma-separated broker list, e.g. localhost:9092                   |
| `--partition <number> (repeatable)` | partition index to query (repeatable; defaults to every partition) |
| `--broker-id <string>`              | query a specific replica broker instead of the partition leaders   |

## Examples

```sh
kafka topic producers orders --brokers localhost:9092
kafka topic producers orders --partition 0 --partition 1 --brokers localhost:9092
```
