---
title: 'group remove-members'
description: "Remove one or more static members from a consumer group's session"
order: 40
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka group remove-members <groupId>
```

Remove one or more static members from a consumer group's session

## Flags

| Flag                             | Description                                                                                |
| -------------------------------- | ------------------------------------------------------------------------------------------ |
| `--brokers <string>`             | comma-separated broker list, e.g. localhost:9092                                           |
| `--yes`                          | confirm the removal without an interactive prompt                                          |
| `--member <string> (repeatable)` | member to remove, memberId or memberId:groupInstanceId (repeatable; at least one required) |

## Examples

```sh
kafka group remove-members my-group --member consumer-1-abc --brokers localhost:9092 --yes
```
