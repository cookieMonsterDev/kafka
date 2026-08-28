---
title: API reference
description: discoverConfigFile, loadConfigFileSync/Async, mergeConfigLayers, createDefineConfig
order: 1
section: reference
---

Everything below is exported from `@cookiemonsterdev/kafka-config`. None of it is Kafka-specific —
the package name reflects where it was extracted from, not a dependency on Kafka.

## Discovery

`discoverConfigFile({ cwd, name?, searchParents?, onDiagnostic? })` searches for a candidate in
this extension order, `.ts` first — TypeScript-first projects run `.ts` natively on modern Node,
so a stray `<name>.config.js` beside a `.ts` file is almost always a stale build artifact:

```
<name>.config.ts → .mts → .cts → .js → .mjs → .cjs → .json
```

`name` defaults to `'kafka'`. Pass your own to discover a differently-named file — e.g.
`discoverConfigFile({ cwd, name: 'app' })` looks for `app.config.ts`.

If none of those exist in a directory, the same ladder is tried under `.config/<name>.*`
(`.config/app.ts`, and so on). A top-level `<name>.config.*` always wins over a `.config/<name>.*`
in the same directory.

- The **first directory** containing any candidate wins entirely — configs at different levels
  are never merged.
- Two candidates in the same directory (e.g. both `app.config.ts` and `app.config.js`): the first
  one in ladder order wins, and a `config.multiple-candidates` diagnostic names both — this
  ambiguity is never silent.
- Search walks **upward** from `cwd` and stops, inclusive of that directory, at the first `.git`,
  `pnpm-workspace.yaml`, or `package.json` carrying a `workspaces` field. Pass
  `searchParents: false` to check only `cwd`.
- No `process.chdir` is ever used — every search takes an explicit `cwd`.

## Loading

`loadConfigFileSync<T>(path, options?)` loads a resolved path — `require()` for
`.ts`/`.mts`/`.cts`/`.js`/`.mjs`/`.cjs`, `JSON.parse` for `.json` — and resolves a sync factory
export. Results are memoised per resolved absolute path, so N callers pay the load cost once per
process.

`T` defaults to `Record<string, unknown>`. Pass `options.assertValid` — an
`(value: unknown) => asserts value is T` function — to validate the resolved value against your
own shape; without one, any plain object is accepted.

`loadConfigFileAsync<T>(path, options?)` is the `import()`-based sibling: it awaits a sync or async
factory export and additionally handles a config module that uses top-level `await`, which the
synchronous path cannot.

```ts
import { discoverConfigFile, loadConfigFileSync } from '@cookiemonsterdev/kafka-config';

interface AppConfig {
  port?: number;
}

function assertValid(value: unknown): asserts value is AppConfig {
  if (typeof value !== 'object' || value === null) {
    throw new TypeError('app.config: expected an object');
  }
}

const path = discoverConfigFile({ cwd: process.cwd(), name: 'app' });
const config = path == null ? {} : loadConfigFileSync<AppConfig>(path, { assertValid });
```

## Defining a config file

`createDefineConfig({ objectSections })` builds a `defineConfig` + `assertValid` pair scoped to
your own known top-level sections:

```ts
import { createDefineConfig } from '@cookiemonsterdev/kafka-config';

interface AppConfig {
  server?: { port?: number };
  logging?: { level?: string };
}

const { defineConfig, assertValid } = createDefineConfig<AppConfig>({
  objectSections: ['server', 'logging'],
});

// app.config.ts
export default defineConfig({
  server: { port: 4000 },
});
```

`defineConfig` is identity, freeze, and shallow validation of the sections you named — it does
**not** throw on an unrecognized top-level key, so an older reader doesn't reject a config file
written for a newer one. A bare object and a sync or async factory are both accepted:

```ts
export default defineConfig(async () => ({
  server: { port: await resolvePortFromSomewhere() },
}));
```

`assertValid` is the same validator `defineConfig` uses internally, exported separately so you can
validate an already-resolved value (for example, inject it into `loadConfigFileSync`'s
`assertValid` option, as core does for its own `KafkaFileConfig`).

## Merging layers

`mergeConfigLayers(override, base, options?)` is the pure function higher-level resolution is
built on. For every key, the highest layer where the value is `!== undefined` wins — `undefined`
means "absent", never "unset to falsy": `0`, `false`, and `''` all survive. A key defined in
neither layer is **omitted** from the result (not set to `undefined`), so destructuring it against
your own default still works.

Every key is replaced atomically by default — merging two values field-by-field is only safe when
you know the shape (a discriminated union or an array, for instance, usually isn't). Pass
`options.shallowMergeKeys` to merge specific keys one level deep instead:

```ts
mergeConfigLayers(
  { retry: { retries: 10 } },
  { retry: { retries: 5, maxRetryTime: 30_000 } },
  {
    shallowMergeKeys: ['retry'],
  },
);
// => { retry: { retries: 10, maxRetryTime: 30_000 } }
```

## The erasable-TypeScript constraint

Node's built-in `.ts` support only **strips** types by default; it does not transform constructs
that need real codegen. A config file (or anything it imports) using a TypeScript `enum`, a
relative import missing its file extension, or `export default` in a `.ts` file whose nearest
`package.json` doesn't declare `"type": "module"`, fails on that default path.

By default, **the synchronous loader** (`loadConfigFileSync`) rescues all three cases: it installs
synchronous `require()` hooks (`module.registerHooks` + `stripTypeScriptTypes({ mode:
'transform' })`) and retries — once per process, and only when the rescue is actually needed,
never on the happy path. The rescue is never silent: a warning names the file and the exact fix
(replace the `enum` with a frozen object; add the `.ts` extension).

**`loadConfigFileAsync` does not get this rescue.** `registerHooks` only intercepts CommonJS
`require()`, so it has no effect on `import()`. A config that needs both async loading (top-level
`await`, or an async factory) _and_ one of the three rescuable constructs has no working path
today — restructure it to avoid needing both at once.

Pass `allowTransformFallback: false` to `loadConfigFileSync` for CI: the original failure surfaces
as an error instead, with the same rewritten, fix-naming message, and the hooks are never
installed — **as long as no earlier call in the same process already installed them.**
`module.registerHooks` has no `deregister`, so once any earlier lenient call (the default) rescues
a file, every later call in that process — even one passing `allowTransformFallback: false` — can
silently succeed against a rescuable file too, because `require()` itself now transparently
rescues it. For the guarantee to be airtight, set `allowTransformFallback: false` on every call
from the start of the process; don't mix it with a lenient call against a potentially-rescuable
file earlier in the same run.

Prefer avoiding the fallback where you can — a rescued config loads through this loader but not
under `node app.config.ts` or `tsx` directly, so this keeps your config file portable:

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
`[kafka-config]`; `'info'` diagnostics (`config.loaded`, and `config.multiple-candidates` when
it's not also escalated) are silent unless you supply your own callback. Codes in use today:
`config.loaded`, `config.multiple-candidates`, `config.transform-fallback`.

## Errors

`KafkaConfigError` is raised while discovering, loading, or parsing a config file. `.tag` names
the specific failure (`'ConfigFileNotFound'`, `'ConfigLoadError'`, `'ConfigFileInvalid'`,
`'UnsupportedExtension'`) so callers can branch without parsing `.message`.

`KafkaConfigRequiresAsyncError` is raised when a config file (or something it imports) uses
top-level `await`, which `loadConfigFileSync` cannot handle — use `loadConfigFileAsync` instead.

Match errors by `.name` (`'KafkaConfigError'` / `'KafkaConfigRequiresAsyncError'`), not
`instanceof` — if your project ends up with two installed copies of this package (a mismatched
version somewhere in the dependency tree), the classes are distinct objects even though the errors
behave identically.

## What this package does **not** do

- **No automatic env reading.** The loader never touches `process.env` at all.
- **No remote or inherited config.** No remote `extends`, no config inheritance chains, no
  YAML/TOML config — only the extension ladder above.
- **No secret redaction.** This loader returns whatever the config file exports, verbatim;
  redacting secrets before printing them is the caller's job.
