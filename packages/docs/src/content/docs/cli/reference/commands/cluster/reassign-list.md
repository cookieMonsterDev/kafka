---
title: 'cluster reassign list'
description: 'List every active partition reassignment'
order: 28
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka cluster reassign list
```

List every active partition reassignment

## Flags

| Flag                 | Description                                      |
| -------------------- | ------------------------------------------------ |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092 |
| `--timeout <number>` | request timeout in ms                            |

## Examples

```sh
kafka cluster reassign list --brokers localhost:9092
```
