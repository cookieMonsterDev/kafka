---
title: 'acl remove'
description: 'Delete every ACL matching a filter, per principal'
order: 18
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka acl remove <principals...>
```

Delete every ACL matching a filter, per principal

## Flags

| Flag                         | Description                                        |
| ---------------------------- | -------------------------------------------------- |
| `--brokers <string>`         | comma-separated broker list, e.g. localhost:9092   |
| `--resource-type <string>`   | limit to this resource type (default: any)         |
| `--resource-name <string>`   | limit to this exact resource name                  |
| `--pattern-type <string>`    | limit to this resource pattern type (default: any) |
| `--host <string>`            | limit to this host                                 |
| `--operation <string>`       | limit to this operation (default: any)             |
| `--permission-type <string>` | limit to this permission type (default: any)       |
| `--yes`                      | confirm the deletion without an interactive prompt |

## Examples

```sh
kafka acl remove User:alice --resource-type topic --resource-name orders --brokers localhost:9092 --yes
```
