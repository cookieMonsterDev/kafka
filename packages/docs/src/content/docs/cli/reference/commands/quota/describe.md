---
title: 'quota describe'
description: 'Describe client quotas matching an entity filter'
order: 43
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka quota describe
```

Describe client quotas matching an entity filter

## Flags

| Flag                                 | Description                                                                         |
| ------------------------------------ | ----------------------------------------------------------------------------------- |
| `--brokers <string>`                 | comma-separated broker list, e.g. localhost:9092                                    |
| `--entity <key=value> (repeatable)`  | an entity type=name to match exactly, or type= for its cluster default (repeatable) |
| `--entity-any <string> (repeatable)` | an entity type to match with any specified name (repeatable)                        |
| `--strict`                           | reject entity types with no filter component                                        |

## Examples

```sh
kafka quota describe --entity user=alice --brokers localhost:9092
kafka quota describe --entity-any client-id --brokers localhost:9092
```
