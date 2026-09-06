---
title: 'cluster raft-voter add'
description: 'Add a voter to the KRaft metadata quorum'
order: 26
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka cluster raft-voter add
```

Add a voter to the KRaft metadata quorum

## Flags

| Flag                               | Description                                                       |
| ---------------------------------- | ----------------------------------------------------------------- |
| `--brokers <string>`               | comma-separated broker list, e.g. localhost:9092                  |
| `--voter-id <number>`              | the new voter’s node id                                           |
| `--voter-directory-id <string>`    | the new voter’s directory id, as a uuid                           |
| `--listener <string> (repeatable)` | a listener the voter is reachable on, name=host:port (repeatable) |
| `--cluster-id <string>`            | expected cluster id, if any                                       |
| `--timeout-ms <number>`            | request timeout in ms                                             |
| `--ack-when-committed`             | wait for the addition to be committed before returning            |
| `--dry-run`                        | print the voter that would be added and exit without connecting   |

## Examples

```sh
kafka cluster raft-voter add --voter-id 4 --voter-directory-id 3c48b... --listener CONTROLLER=localhost:9093 --brokers localhost:9092
```
