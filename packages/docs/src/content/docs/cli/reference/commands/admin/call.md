---
title: 'admin call'
description: 'Call any Admin method by name — the escape hatch for everything without its own command'
order: 18
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka admin call <method>
```

Call any Admin method by name — the escape hatch for everything without its own command _(unstable)_

## Flags

| Flag                   | Description                                                           |
| ---------------------- | --------------------------------------------------------------------- |
| `--brokers <string>`   | comma-separated broker list, e.g. localhost:9092                      |
| `--from-file <string>` | path to a JSON file with the method arguments                         |
| `--yes`                | confirm a non-read-only method                                        |
| `--force`              | confirm a non-read-only method (required alongside --yes)             |
| `--show-secrets`       | print credential fields (password, hmac, …) instead of redacting them |

## Examples

```sh
kafka admin call listTopics --brokers localhost:9092
kafka admin call createAcls --from-file ./acls.json --yes --force
```
