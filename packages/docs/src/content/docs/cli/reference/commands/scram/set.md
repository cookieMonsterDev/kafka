---
title: 'scram set'
description: 'Create or update a SCRAM credential for one or more users'
order: 47
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka scram set <users...>
```

Create or update a SCRAM credential for one or more users

## Flags

| Flag                    | Description                                                   |
| ----------------------- | ------------------------------------------------------------- |
| `--brokers <string>`    | comma-separated broker list, e.g. localhost:9092              |
| `--mechanism <string>`  | scram-sha-256 or scram-sha-512                                |
| `--iterations <number>` | PBKDF2 iteration count (defaults to 4096, the broker minimum) |
| `--password-stdin`      | read the password from stdin — never accepted as a plain flag |

## Examples

```sh
kafka scram set alice --mechanism scram-sha-256 --password-stdin --brokers localhost:9092
```
