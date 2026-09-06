---
title: 'cluster log-dirs'
description: 'Describe log directories and their partition sizes, per broker'
order: 23
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka cluster log-dirs
```

Describe log directories and their partition sizes, per broker

## Flags

| Flag                             | Description                                        |
| -------------------------------- | -------------------------------------------------- |
| `--brokers <string>`             | comma-separated broker list, e.g. localhost:9092   |
| `--broker <number> (repeatable)` | limit to this broker id (repeatable; default: all) |
| `--topic <string> (repeatable)`  | limit to this topic (repeatable; default: all)     |

## Examples

```sh
kafka cluster log-dirs --brokers localhost:9092
kafka cluster log-dirs --topic orders --broker 1 --broker 2
```
