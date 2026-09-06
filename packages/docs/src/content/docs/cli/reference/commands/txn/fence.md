---
title: 'txn fence'
description: "Fence out a transactional id's current producer, bumping its epoch"
order: 65
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka txn fence <transactionalIds...>
```

Fence out a transactional id's current producer, bumping its epoch

## Flags

| Flag                 | Description                                                       |
| -------------------- | ----------------------------------------------------------------- |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092                  |
| `--timeout <number>` | transaction timeout in ms for the fenced producer (default 60000) |

## Examples

```sh
kafka txn fence orders-producer-1 --brokers localhost:9092
```
