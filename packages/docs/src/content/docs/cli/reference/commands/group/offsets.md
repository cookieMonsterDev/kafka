---
title: 'group offsets'
description: "Show a consumer group's committed offsets"
order: 39
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka group offsets <groupId>
```

Show a consumer group's committed offsets

## Flags

| Flag                            | Description                                                                                    |
| ------------------------------- | ---------------------------------------------------------------------------------------------- |
| `--brokers <string>`            | comma-separated broker list, e.g. localhost:9092                                               |
| `--topic <string> (repeatable)` | limit the result to this topic (repeatable; defaults to every topic the group has offsets for) |

## Examples

```sh
kafka group offsets my-group --brokers localhost:9092
kafka group offsets my-group --topic orders
```
