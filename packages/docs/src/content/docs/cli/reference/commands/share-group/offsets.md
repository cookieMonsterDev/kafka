---
title: 'share-group offsets'
description: 'Read, set, or delete a share group’s committed start offsets'
order: 50
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka share-group offsets <groupId>
```

Read, set, or delete a share group’s committed start offsets

## Flags

| Flag                                   | Description                                                          |
| -------------------------------------- | -------------------------------------------------------------------- |
| `--brokers <string>`                   | comma-separated broker list, e.g. localhost:9092                     |
| `--topic <string> (repeatable)`        | limit a read to these topics (repeatable)                            |
| `--set <string> (repeatable)`          | "topic:partition:offset" to set as the new start offset (repeatable) |
| `--delete-topic <string> (repeatable)` | delete committed offsets for this topic (repeatable)                 |
| `--yes`                                | confirm --set/--delete-topic without an interactive prompt           |

## Examples

```sh
kafka share-group offsets orders-readers --brokers localhost:9092
kafka share-group offsets orders-readers --set orders:0:1000 --yes --brokers localhost:9092
kafka share-group offsets orders-readers --delete-topic orders --yes --brokers localhost:9092
```
