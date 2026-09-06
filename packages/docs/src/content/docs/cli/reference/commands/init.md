---
title: 'init'
description: 'Scaffold a kafka.config file in the current directory'
order: 12
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka init
```

Scaffold a kafka.config file in the current directory

## Flags

| Flag      | Description                                                    |
| --------- | -------------------------------------------------------------- |
| `--ts`    | scaffold kafka.config.ts (default when TypeScript is detected) |
| `--js`    | scaffold kafka.config.mjs instead                              |
| `--force` | overwrite the file if it already exists                        |

## Examples

```sh
kafka init
kafka init --js
kafka init --force
```
