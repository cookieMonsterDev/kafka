---
title: 'doctor'
description: 'Report where connection settings come from — config file, environment, profile, or flag'
order: 11
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka doctor
```

Report where connection settings come from — config file, environment, profile, or flag

## Flags

| Flag                 | Description                                          |
| -------------------- | ---------------------------------------------------- |
| `--brokers <string>` | comma-separated broker list, to diagnose as if given |

## Examples

```sh
kafka doctor
kafka doctor --profile staging
```
