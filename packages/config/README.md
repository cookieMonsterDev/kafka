# @cookiemonsterdev/kafka-config

A generic, **zero-runtime-dependency** config-file loader: discovery, sync/async loading, a
TypeScript transform rescue, layer merging, and diagnostics. It has no knowledge of Kafka, or of
any other specific consumer — that knowledge is injected via four extension points (below).

`@cookiemonsterdev/kafka-core` builds its `KafkaFileConfig` / `defineConfig` / `loadKafkaConfig`
facade on top of this package. See
[the docs](https://cookiemonsterdev.github.io/kafka/config/reference/api/) for the full API
reference.

## Install

```sh
npm install @cookiemonsterdev/kafka-config
```

## Quick example

```ts
import { createDefineConfig, discoverConfigFile, loadConfigFileSync } from '@cookiemonsterdev/kafka-config';

interface AppConfig {
  server?: { port?: number };
}

const { defineConfig, assertValid } = createDefineConfig<AppConfig>({ objectSections: ['server'] });

// app.config.ts
export default defineConfig({ server: { port: 4000 } } satisfies AppConfig);

// elsewhere
const path = discoverConfigFile({ cwd: process.cwd(), name: 'app' });
const config = path == null ? {} : loadConfigFileSync<AppConfig>(path, { assertValid });
```

## What's exported

- `discoverConfigFile`, `CANDIDATE_EXTENSIONS` — find a `<name>.config.*` / `.config/<name>.*` file.
- `loadConfigFileSync`, `loadConfigFileAsync` — load and validate one, once resolved.
- `createDefineConfig` — build a `defineConfig` + `assertValid` pair for your own config shape.
- `mergeConfigLayers` — merge two config layers with `undefined`-is-absent semantics.
- `KafkaConfigError`, `KafkaConfigRequiresAsyncError` — typed, `.name`-matchable errors. Match by
  `.name`, not `instanceof` — if your project ends up with two installed copies of this package,
  the classes are distinct objects even though the errors behave identically.
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
