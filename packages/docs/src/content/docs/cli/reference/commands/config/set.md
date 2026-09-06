---
title: 'config set'
description: 'Set one or more config entries on a resource'
order: 34
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka config set <names...>
```

Set one or more config entries on a resource

## Flags

| Flag                               | Description                                                           |
| ---------------------------------- | --------------------------------------------------------------------- |
| `--brokers <string>`               | comma-separated broker list, e.g. localhost:9092                      |
| `--type <string>`                  | resource type: topic, broker, broker-logger, client-metrics, or group |
| `--entry <key=value> (repeatable)` | a config entry to set, key=value (repeatable)                         |
| `--dry-run`                        | validate without changing anything                                    |

## Examples

```sh
kafka config set --type topic orders --entry retention.ms=604800000
kafka config set --type topic orders payments --entry cleanup.policy=compact --dry-run
```
