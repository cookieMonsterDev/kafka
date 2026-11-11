---
title: Global options and defaults
description: Shared connection, output, logging, and help options for every command
order: 2
section: reference
---

Global options apply to all commands. Command pages list operation-specific flags and arguments.
For exact validation and optional arguments, run `kafka <command> --help`.

## Connection and profiles

- `--profile <name>` selects a named profile; `KAFKA_PROFILE` can select it through the environment.
- `--config-file <path>` selects an explicit config file; `KAFKA_CONFIG` is the environment equivalent.
- Connecting commands accept `--brokers <addresses>`. Flag values override environment,
  profile, config-file client settings, then built-in defaults.

Run `kafka doctor` to inspect which values actually won. See [Configuration](../../guides/configuration/).
Topic creation can use `cli.topicDefaults`; when omitted there as well, partition and replication
counts follow the client/broker contract. Set both explicitly for predictable scripts.

## Output and logging

`--json` requests one JSON document on stdout; `--format json` is the explicit format form.
Human output is the default. `--quiet` suppresses informational chatter and `--verbose` adds
diagnostics. Use `--color` / `--no-color` to override terminal color detection.
See [Output and scripting](../../guides/output-and-scripting/) for stdout/stderr contracts and failures.

## Help and version

```sh
kafka --help
kafka topic create --help
kafka --version
```

## Example output

For a successful `kafka topic create docs-demo-events --partitions 1 --replication-factor 1 --json`,
the JSON result has this shape:

```json
{ "results": [{ "topic": "docs-demo-events", "ok": true }] }
```

Runtime-dependent results such as broker ids, offsets, and lag vary by cluster.
Always check the [exit code](../exit-codes/), including partial batch failure.

## Destructive operations

`--yes` skips interactive confirmation where supported. It does not select a cluster or make
an operation reversible. `--force` is command-specific and can bypass additional protections.
Offset reset previews unless `--execute` is supplied; topic deletion has no preview flag.
