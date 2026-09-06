---
title: 'config describe'
description: 'Describe the configs of one or more resources'
order: 31
section: reference
hidden: true
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

```sh
kafka config describe <names...>
```

Describe the configs of one or more resources

## Flags

| Flag                                  | Description                                                           |
| ------------------------------------- | --------------------------------------------------------------------- |
| `--brokers <string>`                  | comma-separated broker list, e.g. localhost:9092                      |
| `--type <string>`                     | resource type: topic, broker, broker-logger, client-metrics, or group |
| `--config-name <string> (repeatable)` | limit the result to this config key (repeatable, default: every key)  |
| `--include-synonyms`                  | include each entry's config synonyms                                  |
| `--include-documentation`             | include each entry's documentation string                             |
| `--show-secrets`                      | print a sensitive config value instead of redacting it                |

## Examples

```sh
kafka config describe --type topic orders --brokers localhost:9092
kafka config describe --type broker 1 --include-synonyms --brokers localhost:9092
```
