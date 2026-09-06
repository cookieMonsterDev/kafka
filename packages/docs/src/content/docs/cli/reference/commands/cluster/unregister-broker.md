---
title: 'cluster unregister-broker'
description: 'Unregister a broker from the cluster (KRaft broker decommissioning)'
order: 29
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka cluster unregister-broker
```

Unregister a broker from the cluster (KRaft broker decommissioning)

## Flags

| Flag                   | Description                                              |
| ---------------------- | -------------------------------------------------------- |
| `--brokers <string>`   | comma-separated broker list, e.g. localhost:9092         |
| `--broker-id <number>` | id of the broker to unregister                           |
| `--yes`                | confirm the unregistration without an interactive prompt |

## Examples

```sh
kafka cluster unregister-broker --broker-id 3 --brokers localhost:9092 --yes
```
