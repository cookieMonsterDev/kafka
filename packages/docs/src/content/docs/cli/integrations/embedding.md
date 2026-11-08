---
title: Embedding the CLI
description: Reuse Kafka CLI commands, help, completion and formatting in a host application
section: integrations
order: 1
---

The `@cookiemonsterdev/kafka-cli/embedded` entry point exposes the CLI engine for hosts such as
Kafka Studio. It runs against a host-provided runtime rather than discovering configuration or
starting an operating-system shell.

```ts
import {
  runEmbeddedCli,
  completeEmbeddedCli,
  embeddedCliConfig,
  embeddedCommandCatalog,
  type EmbeddedCliRuntime,
} from '@cookiemonsterdev/kafka-cli/embedded';

// runtime supplies argv, stdio, connection factory, signal, clock and config loader.
const code = await runEmbeddedCli(runtime, { readOnly: true });
const suggestions = completeEmbeddedCli(['topic', 'c']); // ['create']
const config = embeddedCliConfig(alreadyLoadedFile, activeProfile, configPath);
```

`EmbeddedCliRuntime` is the same port used by the standalone dispatcher: `argv`, `env`, `cwd`,
`stdout`/`stderr` writers, a `stdin` reader, `isTty`, `columns`, `now`, `exit`, `signal`,
`loadConfig` and `openAdmin`. A host must supply its own configuration and return connected Admin
clients from `openAdmin`; commands disconnect those clients after use. Do not return a shared
pooled connection that other callers still own. `exit` should throw rather than terminate the host.

`embeddedCliConfig(fileConfig, profile, path?)` retains CLI defaults from an already-loaded file.
`embeddedCommandCatalog` contains serializable path, summary, positional and flag metadata.
`completeEmbeddedCli(words, additionalCommands?)` derives static completions from the same registry.
The final word is the token being completed, possibly empty. Hosts can augment these suggestions
with their own topic/group discovery.

`runEmbeddedCli(runtime, { readOnly, additionalCommands? })` returns the CLI exit code and writes
through the supplied writers. Local launch/file commands, connection override flags, file-input
flags, secret-display flags and credential-stdin flags are excluded. Read-only enforcement wraps
actual Admin method calls, including passthrough methods; new methods default to being denied.
Destructive commands retain confirmation and independent force checks. Interactive hosts must
supply terminal stdin to answer prompts; otherwise the CLI requires `--yes` as usual.

Hosts can register `EmbeddedCommandSpec` commands with `additionalCommands`. The host owns
read-only enforcement for any extra operation that bypasses `runtime.openAdmin` (for example,
Studio's producer helper). `EmbeddedCliUsageError` and `EmbeddedCliAbortedError` let these
commands report the CLI's standard usage and cancellation codes.

For a signal, abort the runtime's controller and release connected clients. A command may still
be unwinding when dispatch reports cancellation, so the host should also own connection cleanup.
Studio isolates its line editor's terminal environment in a worker, streams output over its
authenticated API, and closes sessions when their connection profile changes.
