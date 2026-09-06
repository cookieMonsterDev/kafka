---
title: 'config unset'
description: 'Remove one or more config entries from a resource, reverting them to default'
order: 35
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka config unset <names...>
```

Remove one or more config entries from a resource, reverting them to default

## Flags

| Flag                          | Description                                                           |
| ----------------------------- | --------------------------------------------------------------------- |
| `--brokers <string>`          | comma-separated broker list, e.g. localhost:9092                      |
| `--type <string>`             | resource type: topic, broker, broker-logger, client-metrics, or group |
| `--key <string> (repeatable)` | a config key to remove (repeatable)                                   |
| `--dry-run`                   | validate without changing anything                                    |

## Examples

```sh
kafka config unset --type topic orders --key retention.ms
kafka config unset --type topic orders payments --key cleanup.policy --dry-run
```
