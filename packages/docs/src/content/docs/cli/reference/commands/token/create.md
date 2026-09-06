---
title: 'token create'
description: 'Create a delegation token'
order: 51
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka token create
```

Create a delegation token

## Flags

| Flag                              | Description                                                                 |
| --------------------------------- | --------------------------------------------------------------------------- |
| `--brokers <string>`              | comma-separated broker list, e.g. localhost:9092                            |
| `--owner <string>`                | owning principal, "PrincipalType:name" (defaults to the authenticated user) |
| `--renewer <string> (repeatable)` | a principal allowed to renew the token, "PrincipalType:name" (repeatable)   |
| `--max-life-time-ms <string>`     | maximum token lifetime in ms (defaults to the broker setting)               |
| `--show-secrets`                  | print the token hmac instead of redacting it                                |

## Examples

```sh
kafka token create --renewer User:alice --brokers localhost:9092 --show-secrets
```
