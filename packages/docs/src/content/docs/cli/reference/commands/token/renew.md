---
title: 'token renew'
description: 'Renew a delegation token, extending its expiry'
order: 54
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka token renew
```

Renew a delegation token, extending its expiry

## Flags

| Flag                              | Description                                                     |
| --------------------------------- | --------------------------------------------------------------- |
| `--brokers <string>`              | comma-separated broker list, e.g. localhost:9092                |
| `--hmac <string>`                 | the token's hmac, base64 (mutually exclusive with --hmac-stdin) |
| `--hmac-stdin`                    | read the token's hmac (base64) from stdin                       |
| `--renew-time-period-ms <string>` | how long to extend the expiry by, in ms                         |

## Examples

```sh
kafka token renew --hmac-stdin --renew-time-period-ms 86400000 --brokers localhost:9092
```
