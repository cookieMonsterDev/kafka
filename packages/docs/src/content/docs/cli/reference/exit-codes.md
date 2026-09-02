---
title: Exit codes
description: The exit-code taxonomy every command shares
order: 2
section: reference
---

Every command resolves to one of these — frozen for the life of the 1.x line. `8`–`69` are
reserved for a future command-specific code; nothing in this package assigns one yet.

| Code  | Meaning                             |
| ----- | ----------------------------------- |
| `0`   | ok                                  |
| `1`   | operation failed                    |
| `2`   | usage error                         |
| `3`   | config error                        |
| `4`   | partial batch failure               |
| `5`   | aborted / unconfirmed               |
| `6`   | unsupported by the connected broker |
| `7`   | authentication failed               |
| `70`  | internal bug — please report it     |
| `130` | interrupted (`SIGINT`)              |
| `143` | terminated (`SIGTERM`)              |

`4` is specific to this CLI's fan-out commands — one that takes multiple topics, principals, or
ids and issues one call per item. It means some items succeeded and others didn't; see
[Output and scripting with JSON](../guides/output-and-scripting/) for how to tell which from the
`--json` output. `5` covers both an interactive "no" at a confirmation prompt and a destructive
command run without `--yes` off a TTY — the same code either way, since a script needs to treat
them identically.
