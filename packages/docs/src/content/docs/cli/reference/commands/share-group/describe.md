---
title: 'share-group describe'
description: 'Describe one or more share groups'
order: 49
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka share-group describe <groupIds...>
```

Describe one or more share groups

## Flags

| Flag                 | Description                                      |
| -------------------- | ------------------------------------------------ |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092 |

## Examples

```sh
kafka share-group describe orders-readers --brokers localhost:9092
```
