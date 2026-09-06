---
title: 'txn list'
description: 'List transactions known to the cluster, optionally filtered'
order: 67
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka txn list
```

List transactions known to the cluster, optionally filtered

## Flags

| Flag                                         | Description                                                 |
| -------------------------------------------- | ----------------------------------------------------------- |
| `--brokers <string>`                         | comma-separated broker list, e.g. localhost:9092            |
| `--state-filter <string> (repeatable)`       | a transaction state to filter on, e.g. Ongoing (repeatable) |
| `--producer-id-filter <string> (repeatable)` | a producer id to filter on (repeatable)                     |
| `--duration-filter <string>`                 | minimum transaction duration in ms                          |
| `--transactional-id-pattern <string>`        | a transactional id pattern to filter on                     |

## Examples

```sh
kafka txn list --brokers localhost:9092
kafka txn list --state-filter Ongoing --brokers localhost:9092
```
