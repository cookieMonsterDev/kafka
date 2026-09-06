---
title: 'group delete-offsets'
description: "Delete a consumer group's committed offsets on one or more topics"
order: 37
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka group delete-offsets <groupId>
```

Delete a consumer group's committed offsets on one or more topics

## Flags

| Flag                                | Description                                                                                          |
| ----------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `--brokers <string>`                | comma-separated broker list, e.g. localhost:9092                                                     |
| `--yes`                             | confirm the deletion without an interactive prompt                                                   |
| `--topic <string> (repeatable)`     | topic to clear (repeatable; at least one required)                                                   |
| `--partition <number> (repeatable)` | partition to clear, applied to every --topic (repeatable; defaults to every partition of each topic) |

## Examples

```sh
kafka group delete-offsets my-group --topic orders --brokers localhost:9092 --yes
```
