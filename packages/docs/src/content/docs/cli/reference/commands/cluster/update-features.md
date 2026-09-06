---
title: 'cluster update-features'
description: 'Upgrade, safe-downgrade, or unsafe-downgrade one or more finalized feature versions'
order: 30
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka cluster update-features
```

Upgrade, safe-downgrade, or unsafe-downgrade one or more finalized feature versions

## Flags

| Flag                                 | Description                                                                                 |
| ------------------------------------ | ------------------------------------------------------------------------------------------- |
| `--brokers <string>`                 | comma-separated broker list, e.g. localhost:9092                                            |
| `--feature <key=value> (repeatable)` | a feature to update, name=maxVersionLevel (repeatable)                                      |
| `--upgrade-type <string>`            | upgrade, safe-downgrade, or unsafe-downgrade (default: upgrade), applied to every --feature |
| `--timeout <number>`                 | request timeout in ms                                                                       |
| `--dry-run`                          | validate on the broker without finalizing anything                                          |
| `--yes`                              | confirm the update without an interactive prompt                                            |
| `--force`                            | required in addition to --yes for an unsafe downgrade                                       |

## Examples

```sh
kafka cluster update-features --feature kraft.version=1 --brokers localhost:9092 --yes
kafka cluster update-features --feature kraft.version=0 --upgrade-type unsafe-downgrade --yes --force --brokers localhost:9092
```
