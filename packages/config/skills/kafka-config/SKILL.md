---
name: kafka-config
description: >
  Use when building a tool or app that discovers and loads its own `<name>.config.ts` file with
  @cookiemonsterdev/kafka-config (discoverConfigFile, loadConfigFileAsync, createDefineConfig,
  mergeConfigLayers), or when a kafka.config.ts used by @cookiemonsterdev/kafka-core, the kafka
  CLI, or the studio fails to load or is not found (ConfigLoadError, enum, extensionless import,
  export default in CommonJS, top-level await, world-writable directory).
metadata:
  purpose: >-
    Uses the generic config-file loader correctly in a new consumer, and diagnoses config files
    that fail under Node's type-stripping TypeScript support.
  type: core
  library: '@cookiemonsterdev/kafka-config'
  library_version: '1.2.1'
sources:
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/config/start/introduction.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/config/reference/api.md'
---

# kafka-config

A generic, zero-dependency config-file loader: discovery, async loading, layer merging, and
diagnostics. Despite the name it has no Kafka code. `@cookiemonsterdev/kafka-core`, the CLI, and
the studio use it to load `kafka.config.*`. To configure a Kafka client, use the
`kafka-core-setup` skill instead; read this one to build your own config loading or to debug a
config file that will not load.

## Setup

Requires Node.js 24+ or Bun 1.4+.

```sh
npm install @cookiemonsterdev/kafka-config
```

```ts
import { createDefineConfig, discoverConfigFile, loadConfigFileAsync } from '@cookiemonsterdev/kafka-config';

// A type alias, not an interface: T must be assignable to Record<string, unknown>.
type AppConfig = {
  server?: { port?: number };
  logging?: { level?: string };
};

export const { defineConfig, assertValid } = createDefineConfig<AppConfig>({
  objectSections: ['server', 'logging'],
});

const path = discoverConfigFile({ cwd: process.cwd(), name: 'app' });
const config = path == null ? {} : await loadConfigFileAsync<AppConfig>(path, { assertValid });
```

Users then write `app.config.ts` with `export default defineConfig({ server: { port: 4000 } })`,
or with a sync or async factory.

## Core Patterns

### Discovery rules

- Extension order: `<name>.config.ts → .mts → .cts → .js → .mjs → .cjs → .json`, then the same
  under `.config/<name>.*`. `name` defaults to `'kafka'`.
- Walks upward from `cwd` and stops at the first `.git`, `pnpm-workspace.yaml`, or `package.json`
  with `workspaces`. The first directory with any candidate wins; levels are never merged.
  `searchParents: false` checks only `cwd`.
- Never enters a world-writable directory (such as `/tmp`) or one owned by another user, and
  returns `null` there. Running as root over a checkout owned by another uid (common in CI
  containers) therefore finds nothing: pass the path explicitly.

### Merge layers

```ts
import { mergeConfigLayers } from '@cookiemonsterdev/kafka-config';

type Settings = { retry?: { retries?: number; maxRetryTime?: number } };

const merged = mergeConfigLayers<Settings>(
  { retry: { retries: 10 } },
  { retry: { retries: 5, maxRetryTime: 30_000 } },
  { shallowMergeKeys: ['retry'] },
);
// { retry: { retries: 10, maxRetryTime: 30_000 } }
```

`undefined` means absent; `0`, `false`, and `''` win. Keys are replaced whole unless listed in
`shallowMergeKeys`.

### Diagnose a config file that fails to load

Node only strips TypeScript types. `loadConfigFileAsync` throws `ConfigLoadError` naming the file
and the fix for:

| Construct in the config or anything it imports        | Fix                                                                          |
| ----------------------------------------------------- | ---------------------------------------------------------------------------- |
| `enum` or other non-erasable syntax                   | A frozen object (`Object.freeze({ Info: 'info' }) as const`) or a union type |
| Relative import without an extension (`'./brokers'`)  | Add the extension (`'./brokers.ts'`)                                         |
| `export default` in a `.ts` file resolved as CommonJS | Rename to `.mts`, or set `"type": "module"` in `package.json`                |

Top-level `await` and async factories work with `loadConfigFileAsync` (and `Kafka.fromConfig()`),
but not with the deprecated synchronous path (`new Kafka()` with a TS/JS file), which throws
`KafkaConfigRequiresAsyncError`.

### Diagnostics and errors

Pass `onDiagnostic` to observe `config.loaded`, `config.multiple-candidates`,
`config.transform-fallback`, and `config.sync-load-deprecated`. Errors are `KafkaConfigError`
with `.tag` (`'ConfigFileNotFound'`, `'ConfigLoadError'`, `'ConfigFileInvalid'`,
`'UnsupportedExtension'`).

```ts
import { loadConfigFileAsync } from '@cookiemonsterdev/kafka-config';

try {
  await loadConfigFileAsync('./app.config.ts');
} catch (error) {
  if (error instanceof Error && error.name === 'KafkaConfigError') {
    console.error((error as Error & { tag?: string }).tag, error.message);
  }
}
```

## Common Mistakes

- **HIGH — Matching errors with `instanceof`.** Two installed copies of the package produce
  distinct classes. Match on `error.name`. Source: config reference/api.
- **HIGH — Using `loadConfigFileSync`.** It is deprecated, cannot handle top-level `await` or
  async factories, and installs process-wide `require()` hooks to rescue broken files. Use
  `loadConfigFileAsync`.
- **MEDIUM — Expecting environment variables to be read.** Discovery and loading never touch
  `process.env`; read env vars in the config file or merge them in as another layer. To load
  `.env` files into `process.env` first, call `loadEnvFiles({ cwd, files: ['.env'] })`
  explicitly (it never overrides variables that are already set).
- **MEDIUM — Printing a loaded config.** There is no secret redaction; redact passwords and
  tokens yourself before logging.
- **MEDIUM — "No config found" in CI or Docker.** Check directory ownership and world-writable
  permissions before assuming the file name is wrong.

## Completion

- `discoverConfigFile` returns the intended path from each entry point's working directory.
- `loadConfigFileAsync` returns the expected object, and an invalid file fails with a
  `KafkaConfigError` whose `tag` and message name the problem.
