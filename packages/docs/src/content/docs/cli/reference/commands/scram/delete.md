---
title: 'scram delete'
description: 'Delete a SCRAM credential for one or more users'
order: 45
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka scram delete <users...>
```

Delete a SCRAM credential for one or more users

## Flags

| Flag                   | Description                                        |
| ---------------------- | -------------------------------------------------- |
| `--brokers <string>`   | comma-separated broker list, e.g. localhost:9092   |
| `--mechanism <string>` | scram-sha-256 or scram-sha-512                     |
| `--yes`                | confirm the deletion without an interactive prompt |

## Examples

```sh
kafka scram delete alice --mechanism scram-sha-256 --brokers localhost:9092 --yes
```
