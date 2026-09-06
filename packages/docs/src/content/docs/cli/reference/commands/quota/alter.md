---
title: 'quota alter'
description: 'Set or remove client quota values for one entity'
order: 42
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka quota alter
```

Set or remove client quota values for one entity

## Flags

| Flag                                | Description                                                                 |
| ----------------------------------- | --------------------------------------------------------------------------- |
| `--brokers <string>`                | comma-separated broker list, e.g. localhost:9092                            |
| `--entity <key=value> (repeatable)` | an entity type=name to alter, or type= for its cluster default (repeatable) |
| `--set <key=value> (repeatable)`    | a quota key=value to set (repeatable)                                       |
| `--unset <string> (repeatable)`     | a quota key to remove (repeatable)                                          |
| `--dry-run`                         | validate without changing anything                                          |

## Examples

```sh
kafka quota alter --entity user=alice --set producer_byte_rate=1048576 --brokers localhost:9092
kafka quota alter --entity user=alice --entity client-id=orders-producer --unset producer_byte_rate --brokers localhost:9092
```
