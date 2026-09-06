---
title: 'cluster raft-voter remove'
description: 'Remove a voter from the KRaft metadata quorum'
order: 27
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka cluster raft-voter remove
```

Remove a voter from the KRaft metadata quorum

## Flags

| Flag                            | Description                                       |
| ------------------------------- | ------------------------------------------------- |
| `--brokers <string>`            | comma-separated broker list, e.g. localhost:9092  |
| `--voter-id <number>`           | the voter’s node id                               |
| `--voter-directory-id <string>` | the voter’s directory id, as a uuid               |
| `--cluster-id <string>`         | expected cluster id, if any                       |
| `--yes`                         | confirm the removal without an interactive prompt |

## Examples

```sh
kafka cluster raft-voter remove --voter-id 4 --voter-directory-id 3c48b... --brokers localhost:9092 --yes
```
