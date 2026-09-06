---
title: 'group describe'
description: 'Describe one or more consumer groups'
order: 38
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka group describe <groupIds...>
```

Describe one or more consumer groups

## Flags

| Flag                 | Description                                      |
| -------------------- | ------------------------------------------------ |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092 |

## Examples

```sh
kafka group describe my-group --brokers localhost:9092
```
