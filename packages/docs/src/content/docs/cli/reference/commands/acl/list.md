---
title: 'acl list'
description: 'List ACLs matching a filter, or every ACL by default'
order: 16
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka acl list
```

List ACLs matching a filter, or every ACL by default

## Flags

| Flag                         | Description                                        |
| ---------------------------- | -------------------------------------------------- |
| `--brokers <string>`         | comma-separated broker list, e.g. localhost:9092   |
| `--resource-type <string>`   | limit to this resource type (default: any)         |
| `--resource-name <string>`   | limit to this exact resource name                  |
| `--pattern-type <string>`    | limit to this resource pattern type (default: any) |
| `--principal <string>`       | limit to this principal, e.g. User:alice           |
| `--host <string>`            | limit to this host                                 |
| `--operation <string>`       | limit to this operation (default: any)             |
| `--permission-type <string>` | limit to this permission type (default: any)       |

## Examples

```sh
kafka acl list --brokers localhost:9092
kafka acl list --resource-type topic --resource-name orders
```
