---
title: 'txn abort'
description: 'Write an abort marker for one in-flight transaction on a topic partition'
order: 63
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka txn abort
```

Write an abort marker for one in-flight transaction on a topic partition

## Flags

| Flag                             | Description                                                    |
| -------------------------------- | -------------------------------------------------------------- |
| `--brokers <string>`             | comma-separated broker list, e.g. localhost:9092               |
| `--topic <string>`               | topic holding the in-flight transactional data                 |
| `--partition <number>`           | partition holding the in-flight transactional data             |
| `--producer-id <string>`         | the transaction's producer id                                  |
| `--producer-epoch <number>`      | the transaction's producer epoch                               |
| `--coordinator-epoch <number>`   | coordinator epoch, if known (resolved automatically otherwise) |
| `--transaction-version <number>` | transaction protocol version, if known                         |
| `--yes`                          | confirm the abort without an interactive prompt                |

## Examples

```sh
kafka txn abort --topic orders --partition 0 --producer-id 1000 --producer-epoch 0 --brokers localhost:9092 --yes
```
