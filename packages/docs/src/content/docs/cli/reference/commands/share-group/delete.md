---
title: 'share-group delete'
description: 'Delete one or more share groups'
order: 48
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka share-group delete <groupIds...>
```

Delete one or more share groups

## Flags

| Flag                 | Description                                        |
| -------------------- | -------------------------------------------------- |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092   |
| `--yes`              | confirm the deletion without an interactive prompt |

## Examples

```sh
kafka share-group delete orders-readers --brokers localhost:9092 --yes
```
