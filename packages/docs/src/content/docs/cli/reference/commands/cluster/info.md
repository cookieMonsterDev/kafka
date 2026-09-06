---
title: 'cluster info'
description: 'Describe the cluster: its brokers, controller, and cluster id'
order: 23
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka cluster info
```

Describe the cluster: its brokers, controller, and cluster id

## Flags

| Flag                 | Description                                      |
| -------------------- | ------------------------------------------------ |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092 |

## Examples

```sh
kafka cluster info --brokers localhost:9092
```
