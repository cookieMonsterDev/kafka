---
title: 'txn describe'
description: 'Describe one or more transactional ids'
order: 65
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka txn describe <transactionalIds...>
```

Describe one or more transactional ids

## Flags

| Flag                 | Description                                      |
| -------------------- | ------------------------------------------------ |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092 |

## Examples

```sh
kafka txn describe orders-producer-1 --brokers localhost:9092
```
