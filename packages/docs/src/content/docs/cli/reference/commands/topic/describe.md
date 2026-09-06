---
title: 'topic describe'
description: 'Describe one or more topics'
order: 60
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka topic describe <topics...>
```

Describe one or more topics

## Flags

| Flag                 | Description                                      |
| -------------------- | ------------------------------------------------ |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092 |

## Examples

```sh
kafka topic describe orders --brokers localhost:9092
```
