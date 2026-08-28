---
title: Config file
description: kafka.config.* discovery, defineConfig, and the loader utilities in @cookiemonsterdev/kafka-config
order: 9
section: reference
---

`kafka.config.*` discovery, loading, and validation is split across two packages:

- **[`@cookiemonsterdev/kafka-config`](https://www.npmjs.com/package/@cookiemonsterdev/kafka-config)**
  — a generic, zero-runtime-dependency loader: discovery, sync/async loading, a TypeScript
  transform rescue, layer merging, and diagnostics. It knows nothing about Kafka.
- **`@cookiemonsterdev/kafka-core`'s `./config` subpath** — this package's Kafka-typed facade on
  top of the loader: `KafkaFileConfig`, a bound `defineConfig`, and `loadKafkaConfig`.

It is a standalone loader today: nothing in this package reads a config file automatically yet.
`new Kafka()` picking up a config file is tracked separately; this page documents the loader
itself.

Source: [`packages/config/src/`](https://github.com/cookieMonsterDev/kafka/tree/master/packages/config/src)
(the generic machinery) and
[`packages/core/src/config/`](https://github.com/cookieMonsterDev/kafka/tree/master/packages/core/src/config)
(core's facade).

> **Migrating from `core-v2.1.0`?** Everything documented on this page used to live entirely under
> `@cookiemonsterdev/kafka-core/config`. That subpath still re-exports the generic machinery for
> compatibility, but every symbol it re-exports is `@deprecated` and will be removed in core
> `3.0.0` — import from `@cookiemonsterdev/kafka-config` directly instead. Core's own facade
> (`defineConfig`, `loadKafkaConfig`, `KafkaFileConfig`) is not deprecated and stays at
> `@cookiemonsterdev/kafka-core/config`.

## Writing a config file

```ts
// kafka.config.ts
import { defineConfig } from '@cookiemonsterdev/kafka-core/config';

export default defineConfig({
  client: {
    brokers: ['localhost:9092'],
  },
});
```

`defineConfig` is identity, freeze, and shallow validation of the known sections — it does **not**
throw on an unrecognized top-level key, so an older core loading a config file written for a newer
CLI version does not break. A bare object and a sync or async factory are both accepted:

```ts
export default defineConfig(async () => ({
  client: { brokers: await resolveBrokersFromSomewhere() },
}));
```

Sections are namespaced so more can be added later without a breaking change:

```ts
interface KafkaFileConfig {
  client?: Omit<KafkaConfig, 'brokers'> & { brokers?: KafkaConfig['brokers'] };
  producer?: ProducerConfig;
  consumer?: Omit<ConsumerConfig, 'groupId'> & { groupId?: string };
  shareConsumer?: Omit<ShareConsumerConfig, 'groupId'> & { groupId?: string };
  admin?: AdminConfig;
  [key: string]: unknown; // forward compatibility
}
```

## Discovery

`discoverConfigFile({ cwd, name?, searchParents?, onDiagnostic? })`, from
`@cookiemonsterdev/kafka-config`, searches for a candidate in this extension order, `.ts` first —
this repo family is TypeScript-first and Node runs `.ts` natively, so a stray `kafka.config.js`
beside a `.ts` file is almost always a stale build artifact:

```
<name>.config.ts → .mts → .cts → .js → .mjs → .cjs → .json
```

`name` defaults to `'kafka'`, which is what `loadKafkaConfig` uses; a different consumer built on
the same loader passes its own name (`kafka-studio` might pass `'studio'` to discover
`studio.config.ts` instead).

If none of those exist in a directory, the same ladder is tried under `.config/<name>.*`
(`.config/kafka.ts`, and so on). A top-level `<name>.config.*` always wins over a `.config/<name>.*`
in the same directory.

- The **first directory** containing any candidate wins entirely — configs at different levels
  are never merged.
- Two candidates in the same directory (e.g. both `kafka.config.ts` and `kafka.config.js`): the
  first one in ladder order wins, and a `config.multiple-candidates` diagnostic names both — this
  ambiguity is never silent.
- Search walks **upward** from `cwd` and stops, inclusive of that directory, at the first `.git`,
  `pnpm-workspace.yaml`, or `package.json` carrying a `workspaces` field. Pass
  `searchParents: false` to check only `cwd`.
- No `process.chdir` is ever used — every search takes an explicit `cwd`.

## Loading

`loadConfigFileSync<T>(path, options?)` (from `@cookiemonsterdev/kafka-config`) loads a resolved
path — `require()` for `.ts`/`.mts`/`.cts`/`.js`/`.mjs`/`.cjs`, `JSON.parse` for `.json` — and
resolves a sync factory export. Results are memoised per resolved absolute path, so N clients pay
the load cost once per process. `T` defaults to `Record<string, unknown>`; pass
`options.assertValid` to validate against your own shape — core's `loadKafkaConfig` injects a
validator for `KafkaFileConfig`.

`loadConfigFileAsync<T>(path, options?)` is the `import()`-based sibling: it awaits a sync or async
factory export and additionally handles a config module that uses top-level `await`, which the
synchronous path cannot.

`loadKafkaConfig(options?)`, from `@cookiemonsterdev/kafka-core/config`, is the Kafka-typed
orchestrator most callers want: it discovers (unless an explicit `path` or `KAFKA_CONFIG` is set),
loads, and **never throws** — every failure comes back as `{ ok: false, error }`, with `error.tag`
one of `'ConfigFileNotFound'`, `'ConfigLoadError'`, `'ConfigFileInvalid'`, or
`'UnsupportedExtension'`. A successful `config` is always a frozen plain object, even when nothing
was found (`{}`).

```ts
import { loadKafkaConfig } from '@cookiemonsterdev/kafka-core/config';

const result = loadKafkaConfig({ cwd: process.cwd() });
if (!result.ok) {
  console.error(result.error.tag, result.error.message);
} else if (result.resolvedPath != null) {
  console.log('Loaded from', result.resolvedPath);
}
```

### `--config` / `KAFKA_CONFIG`

An explicit path takes precedence over discovery. `loadKafkaConfig({ path })` is the programmatic
form; `loadKafkaConfig()` (no `path`) also honors the `KAFKA_CONFIG` environment variable as the
same kind of explicit override, ahead of discovery. A missing explicit path is a hard error
(`'ConfigFileNotFound'`) — it never falls back to searching.

## The erasable-TypeScript constraint

Node's built-in `.ts` support only **strips** types by default; it does not transform constructs
that need real codegen. A config file (or anything it imports) using a TypeScript `enum`, a
relative import missing its file extension, or `export default` in a `.ts` file whose nearest
`package.json` doesn't declare `"type": "module"`, fails on that default path.

By default, **the synchronous loader** (`loadConfigFileSync`, and therefore `loadKafkaConfig`)
rescues all three cases: it installs synchronous `require()` hooks (`module.registerHooks` +
`stripTypeScriptTypes({ mode: 'transform' })`) and retries — once per process, and only when the
rescue is actually needed, never on the happy path. The rescue is never silent: a warning names
the file and the exact fix (replace the `enum` with a frozen object; add the `.ts` extension).

**`loadConfigFileAsync` does not get this rescue.** `registerHooks` only intercepts CommonJS
`require()`, so it has no effect on `import()`. A config that needs both async loading (top-level
`await`, or an async factory) _and_ one of the three rescuable constructs has no working path
today — restructure it to avoid needing both at once.

Pass `allowTransformFallback: false` to `loadConfigFileSync` / `loadKafkaConfig` for CI: the
original failure surfaces as an error instead, with the same rewritten, fix-naming message, and
the hooks are never installed — **as long as no earlier call in the same process already installed
them.** `module.registerHooks` has no `deregister`, so once any earlier lenient call (the default)
rescues a file, every later call in that process — even one passing
`allowTransformFallback: false` — can silently succeed against a rescuable file too, because
`require()` itself now transparently rescues it. For the guarantee to be airtight, set
`allowTransformFallback: false` on every call from the start of the process; don't mix it with a
lenient call against a potentially-rescuable file earlier in the same run.

Prefer avoiding the fallback where you can — a rescued config loads through this loader but not
under `node kafka.config.ts` or `tsx` directly, so this keeps your config file portable:

```ts
// Avoid — needs the transform fallback
enum Level {
  Info = 'info',
}

// Prefer — erasable, loads everywhere
const Level = Object.freeze({ Info: 'info' }) as const;
```

## Diagnostics

Every discovery/load function accepts an `onDiagnostic` callback:
`{ code, level: 'info' | 'warn', message, path?, ...extra }`. The default handler
(`defaultOnConfigDiagnostic`) writes only `'warn'`-level diagnostics to stderr, prefixed
`[kafka-config]` (the package's own name, regardless of which consumer is loading a file);
`'info'` diagnostics (`config.loaded`, and `config.multiple-candidates` when it's not also
escalated) are silent unless you supply your own callback. Codes in use today: `config.loaded`,
`config.multiple-candidates`, `config.transform-fallback`.

## What this package does **not** do

- **No automatic env reading.** The loader never touches `process.env` except for the single,
  explicit `KAFKA_CONFIG` override described above — no `KAFKA_FOO_BAR` → `fooBar` mapping, no
  `.env` file loading. (The pre-existing `KAFKA_LOG_LEVEL` override in `loggers/index.ts` is a
  separate, older, documented wart — not a precedent this loader extends.)
- **No remote or inherited config.** No `giget`-style remote `extends`, no config inheritance
  chains, no YAML/TOML config — only the extension ladder above.
- **No secret redaction here.** This loader returns whatever the config file exports, verbatim;
  redacting secrets before printing them is the caller's job.

## Merging layers

`mergeConfigLayers(override, base, options?)`, from `@cookiemonsterdev/kafka-config`, is the pure
function higher-level resolution is built on, and is exported for anyone assembling layers
themselves. For every key, the highest layer where the value is `!== undefined` wins —
`undefined` means "absent", never "unset to falsy": `0`, `false`, and `''` all survive. A key
defined in neither layer is **omitted** from the result (not set to `undefined`), so destructuring
it against your own default still works.

Every key is replaced atomically by default — merging two `sasl` mechanisms field-by-field would
produce an invalid object, and arrays are never concatenated. Pass
`options.shallowMergeKeys` to merge specific keys one level deep instead; core's own resolution
merges `retry` this way (matching what `client.ts`'s producer/consumer already do), via a small
`mergeKafkaConfigLayers` binding in `@cookiemonsterdev/kafka-core`'s facade.

## Building your own config file on top of `@cookiemonsterdev/kafka-config`

The loader has exactly four extension points, each with a generic default — this is what lets a
future package (`kafka-studio` is the motivating example, not yet built) reuse the same machinery
for its own `studio.config.*` file without forking it:

| Option                                 | Where                        | Default                                              |
| -------------------------------------- | ---------------------------- | ---------------------------------------------------- |
| `name`                                 | `discoverConfigFile`         | `'kafka'`                                            |
| `shallowMergeKeys`                     | `mergeConfigLayers`          | `[]` (everything replaced atomically)                |
| `objectSections`                       | `createDefineConfig`         | required — your known sections                       |
| `T` (the config shape) + `assertValid` | `loadConfigFileSync`/`Async` | `Record<string, unknown>` / accepts any plain object |

```ts
import { createDefineConfig, discoverConfigFile, loadConfigFileSync } from '@cookiemonsterdev/kafka-config';

interface StudioConfig {
  ui?: { port?: number };
}

const { defineConfig, assertValid } = createDefineConfig<StudioConfig>({ objectSections: ['ui'] });

const path = discoverConfigFile({ cwd: process.cwd(), name: 'studio' });
const config = path == null ? {} : loadConfigFileSync<StudioConfig>(path, { assertValid });
```
