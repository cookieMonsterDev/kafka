---
title: 'acl add'
description: 'Create one or more ACLs, granting or denying an operation'
order: 15
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka acl add <principals...>
```

Create one or more ACLs, granting or denying an operation

## Flags

| Flag                                | Description                                                              |
| ----------------------------------- | ------------------------------------------------------------------------ |
| `--brokers <string>`                | comma-separated broker list, e.g. localhost:9092                         |
| `--resource-type <string>`          | resource type: topic, group, cluster, transactional-id, delegation-token |
| `--resource-name <string>`          | exact name of the resource                                               |
| `--pattern-type <string>`           | resource pattern type: literal or prefixed (default: literal)            |
| `--operation <string> (repeatable)` | operation to grant (repeatable), e.g. read, write                        |
| `--host <string>`                   | host the principal connects from (default: *)                            |
| `--permission-type <string>`        | allow or deny (default: allow)                                           |
| `--dry-run`                         | print what would be created without creating anything                    |

## Examples

```sh
kafka acl add User:alice --resource-type topic --resource-name orders --operation read --operation write
kafka acl add User:alice User:bob --resource-type group --resource-name my-group --operation read --dry-run
```
