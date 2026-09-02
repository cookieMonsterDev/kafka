# @cookiemonsterdev/kafka-config

<p>
  <a href="https://github.com/cookieMonsterDev/kafka/actions/workflows/ci.yml"><img src="https://github.com/cookieMonsterDev/kafka/actions/workflows/ci.yml/badge.svg?branch=develop" alt="CI" /></a>
  <a href="https://www.npmjs.com/package/@cookiemonsterdev/kafka-config"><img src="https://img.shields.io/npm/v/@cookiemonsterdev/kafka-config.svg" alt="npm" /></a>
  <a href="../../LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT License" /></a>
</p>

Finds and loads a project's `<name>.config.*` file — think the same idea as `eslint.config.js` or
`vite.config.ts`, but generic enough for you to point at your own app's name. It's a **zero-runtime-
dependency** loader that handles discovery, sync/async loading, a TypeScript transform rescue,
layer merging, and diagnostics. It has no knowledge of Kafka, or of any other specific consumer —
that knowledge is injected via four extension points (below).

`@cookiemonsterdev/kafka-core` doesn't read a config file yet — `new Kafka({...})` still takes its
options directly. This package is what [`@cookiemonsterdev/kafka-cli`](../cli/README.md) uses to
load `kafka.config.ts`, published on its own so anything else (a studio UI, another CLI) can build
the same kind of config-file layer without depending on the rest of this workspace. See
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

## License

[MIT](../../LICENSE) © Mykhailo Toporkov
