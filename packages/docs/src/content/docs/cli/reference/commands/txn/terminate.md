---
title: 'txn terminate'
description: "Force-terminate a transactional id's current transaction"
order: 67
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka txn terminate <transactionalId>
```

Force-terminate a transactional id's current transaction

## Flags

| Flag                 | Description                                           |
| -------------------- | ----------------------------------------------------- |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092      |
| `--timeout <number>` | transaction timeout in ms for the fenced producer     |
| `--yes`              | confirm the termination without an interactive prompt |

## Examples

```sh
kafka txn terminate orders-producer-1 --brokers localhost:9092 --yes
```
