---
title: 'scram list'
description: 'List SCRAM credentials for one or more users, or every user'
order: 45
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka scram list <users...>
```

List SCRAM credentials for one or more users, or every user

## Flags

| Flag                 | Description                                      |
| -------------------- | ------------------------------------------------ |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092 |

## Examples

```sh
kafka scram list --brokers localhost:9092
kafka scram list alice bob --brokers localhost:9092
```
