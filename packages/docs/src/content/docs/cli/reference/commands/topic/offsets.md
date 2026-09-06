---
title: 'topic offsets'
description: 'Show partition offsets for a topic'
order: 62
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka topic offsets <topic>
```

Show partition offsets for a topic

## Flags

| Flag                 | Description                                                                             |
| -------------------- | --------------------------------------------------------------------------------------- |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092                                        |
| `--time <string>`    | resolve offsets as of "earliest", "latest", "max-timestamp", or a millisecond timestamp |

## Examples

```sh
kafka topic offsets orders --brokers localhost:9092
kafka topic offsets orders --time earliest --brokers localhost:9092
kafka topic offsets orders --time 1735689600000 --brokers localhost:9092
```
