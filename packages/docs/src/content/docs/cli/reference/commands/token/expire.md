---
title: 'token expire'
description: 'Expire a delegation token, immediately by default'
order: 53
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka token expire
```

Expire a delegation token, immediately by default

## Flags

| Flag                               | Description                                                     |
| ---------------------------------- | --------------------------------------------------------------- |
| `--brokers <string>`               | comma-separated broker list, e.g. localhost:9092                |
| `--hmac <string>`                  | the token's hmac, base64 (mutually exclusive with --hmac-stdin) |
| `--hmac-stdin`                     | read the token's hmac (base64) from stdin                       |
| `--expiry-time-period-ms <string>` | ms until expiry (defaults to immediate expiry)                  |
| `--yes`                            | confirm the expiry without an interactive prompt                |

## Examples

```sh
kafka token expire --hmac-stdin --brokers localhost:9092 --yes
```
