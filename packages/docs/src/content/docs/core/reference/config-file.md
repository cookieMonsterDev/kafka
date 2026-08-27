---
title: Config file
description: kafka.config.* discovery, defineConfig, and the loader utilities in @cookiemonsterdev/kafka-core/config
order: 9
section: reference
---

`@cookiemonsterdev/kafka-core/config` — a separate subpath, not re-exported from the package
root — discovers, loads, and validates a `kafka.config.*` file. It is a standalone loader today:
nothing in this package reads a config file automatically yet. `new Kafka()` picking up a config
file is tracked separately; this page documents the loader itself.

Source: [`src/config/`](https://github.com/cookieMonsterDev/kafka/tree/master/packages/core/src/config).

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

`discoverConfigFile({ cwd, searchParents?, onDiagnostic? })` searches for a candidate in this
extension order, `.ts` first — this repo family is TypeScript-first and Node runs `.ts` natively,
so a stray `kafka.config.js` beside a `.ts` file is almost always a stale build artifact:

```
kafka.config.ts → .mts → .cts → .js → .mjs → .cjs → .json
```

If none of those exist in a directory, the same ladder is tried under `.config/kafka.*`
(`.config/kafka.ts`, and so on). A top-level `kafka.config.*` always wins over a `.config/kafka.*`
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

`loadConfigFileSync(path)` loads a resolved path — `require()` for `.ts`/`.mts`/`.cts`/`.js`/
`.mjs`/`.cjs`, `JSON.parse` for `.json` — and resolves a sync factory export. Results are memoised
per resolved absolute path, so N clients pay the load cost once per process.

`loadConfigFileAsync(path)` is the `import()`-based sibling: it awaits a sync or async factory
export and additionally handles a config module that uses top-level `await`, which the
synchronous path cannot.

`loadKafkaConfig(options?)` is the orchestrator most callers want: it discovers (unless an
explicit `path` or `KAFKA_CONFIG` is set), loads, and **never throws** — every failure comes back
as `{ ok: false, error }`, with `error.tag` one of `'ConfigFileNotFound'`, `'ConfigLoadError'`,
`'ConfigFileInvalid'`, or `'UnsupportedExtension'`. A successful `config` is always a frozen plain
object, even when nothing was found (`{}`).

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

By default, the loader rescues all three cases: it installs synchronous `require()` hooks
(`module.registerHooks` + `stripTypeScriptTypes({ mode: 'transform' })`) and retries — once per
process, and only when the rescue is actually needed, never on the happy path. The rescue is never
silent: a warning names the file and the exact fix (replace the `enum` with a frozen object; add
the `.ts` extension).

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
`[kafka-config]`; `'info'` diagnostics (`config.loaded`, and `config.multiple-candidates` when it's
not also escalated) are silent unless you supply your own callback. Codes in use today:
`config.loaded`, `config.multiple-candidates`, `config.transform-fallback`.

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

`mergeConfigLayers(override, base)` is the pure function this package's own higher-level
resolution is built on, and is exported for anyone assembling layers themselves. For every key,
the highest layer where the value is `!== undefined` wins — `undefined` means "absent", never
"unset to falsy": `0`, `false`, and `''` all survive. A key defined in neither layer is **omitted**
from the result (not set to `undefined`), so destructuring it against your own default still
works. `retry` is shallow-merged one level; every other key (`sasl`, `ssl`, `brokers`, `metrics`,
`socketFactory`, `logCreator`, and everything else) is replaced atomically — merging two `sasl`
mechanisms field-by-field would produce an invalid object, and arrays are never concatenated.
