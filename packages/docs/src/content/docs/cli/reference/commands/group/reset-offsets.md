---
title: 'group reset-offsets'
description: "Reset a consumer group's committed offsets on one or more topics"
order: 42
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka group reset-offsets <groupId>
```

Reset a consumer group's committed offsets on one or more topics

## Flags

| Flag                            | Description                                           |
| ------------------------------- | ----------------------------------------------------- |
| `--brokers <string>`            | comma-separated broker list, e.g. localhost:9092      |
| `--topic <string> (repeatable)` | topic to reset (repeatable; at least one required)    |
| `--to <string>`                 | reset target: earliest or latest                      |
| `--execute`                     | actually reset the offsets (without it, preview only) |
| `--yes`                         | confirm the reset without an interactive prompt       |

## Examples

```sh
kafka group reset-offsets my-group --topic orders --to earliest --brokers localhost:9092
kafka group reset-offsets my-group --topic orders --to earliest --execute --yes --brokers localhost:9092
```
