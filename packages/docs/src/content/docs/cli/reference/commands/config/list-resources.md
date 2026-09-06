---
title: 'config list-resources'
description: 'List every config resource the broker knows about'
order: 33
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka config list-resources
```

List every config resource the broker knows about

## Flags

| Flag                           | Description                                                   |
| ------------------------------ | ------------------------------------------------------------- |
| `--brokers <string>`           | comma-separated broker list, e.g. localhost:9092              |
| `--type <string> (repeatable)` | limit to this resource type (repeatable, default: every type) |

## Examples

```sh
kafka config list-resources --brokers localhost:9092
kafka config list-resources --type topic --type group
```
