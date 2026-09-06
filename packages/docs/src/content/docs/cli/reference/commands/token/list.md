---
title: 'token list'
description: 'List delegation tokens, optionally filtered by owner'
order: 54
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka token list
```

List delegation tokens, optionally filtered by owner

## Flags

| Flag                            | Description                                                         |
| ------------------------------- | ------------------------------------------------------------------- |
| `--brokers <string>`            | comma-separated broker list, e.g. localhost:9092                    |
| `--owner <string> (repeatable)` | an owning principal to filter on, "PrincipalType:name" (repeatable) |
| `--show-secrets`                | print each token hmac instead of redacting it                       |

## Examples

```sh
kafka token list --brokers localhost:9092
kafka token list --owner User:alice --brokers localhost:9092
```
