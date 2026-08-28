# @cookiemonsterdev/kafka-config

A generic, **zero-runtime-dependency** config-file loader: discovery, sync/async loading, a
TypeScript transform rescue, layer merging, and diagnostics. It has no knowledge of Kafka, or of
any other specific consumer — that knowledge is injected via four extension points (below).

`@cookiemonsterdev/kafka-core` builds its `KafkaFileConfig` / `defineConfig` / `loadKafkaConfig`
facade on top of this package; `@cookiemonsterdev/kafka-cli` reads the same config file through
that facade. See
[the config-file reference](https://cookiemonsterdev.github.io/kafka/core/reference/config-file/)
for the full documentation, including how a future consumer builds its own facade on this loader.

## Install

```sh
npm install @cookiemonsterdev/kafka-config
```

## Quick example

```ts
import { createDefineConfig, discoverConfigFile, loadConfigFileSync } from '@cookiemonsterdev/kafka-config';

interface StudioConfig {
  ui?: { port?: number };
}

const { defineConfig, assertValid } = createDefineConfig<StudioConfig>({ objectSections: ['ui'] });

// studio.config.ts
export default defineConfig({ ui: { port: 4000 } } satisfies StudioConfig);

// elsewhere
const path = discoverConfigFile({ cwd: process.cwd(), name: 'studio' });
const config = path == null ? {} : loadConfigFileSync<StudioConfig>(path, { assertValid });
```

## What's exported

- `discoverConfigFile`, `CANDIDATE_EXTENSIONS` — find a `<name>.config.*` / `.config/<name>.*` file.
- `loadConfigFileSync`, `loadConfigFileAsync` — load and validate one, once resolved.
- `createDefineConfig` — build a `defineConfig` + `assertValid` pair for your own config shape.
- `mergeConfigLayers` — merge two config layers with `undefined`-is-absent semantics.
- `KafkaConfigError`, `KafkaConfigRequiresAsyncError` — typed, `.name`-matchable errors (never
  `instanceof` across a possible duplicate-copy boundary — see D18a in the project's plan).
- `defaultOnConfigDiagnostic`, `ConfigDiagnostic`, `OnConfigDiagnostic` — the diagnostics channel
  every discovery/load function accepts.
- `installConfigTransformHooks`, `areConfigTransformHooksInstalled` — the TypeScript transform
  rescue (enums, extensionless imports, `export default` under a CommonJS-resolved file).

Despite the package name, none of this is Kafka-specific — the name reflects where it was
extracted from, not a Kafka dependency.

## Development

From the repo root:

```sh
pnpm --filter @cookiemonsterdev/kafka-config build
pnpm --filter @cookiemonsterdev/kafka-config test
pnpm --filter @cookiemonsterdev/kafka-config typecheck
```

See the root [CONTRIBUTING.md](../../CONTRIBUTING.md) for the full workspace workflow.
