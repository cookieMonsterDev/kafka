---
title: 'studio'
description: 'Launch the local web UI for browsing and driving the cluster'
order: 15
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka studio
```

Launch the local web UI for browsing and driving the cluster

## Flags

| Flag                  | Description                                               |
| --------------------- | --------------------------------------------------------- |
| `-p, --port <number>` | port to listen on (default: first free port in 5757-5807) |
| `--host <string>`     | address to bind (default: 127.0.0.1)                      |
| `--browser <string>`  | browser command to open, or "none" to disable             |
| `--read-only`         | reject mutating API requests with 403                     |

## Examples

```sh
kafka studio
kafka studio --read-only
kafka studio --port 5757 --browser none
```
