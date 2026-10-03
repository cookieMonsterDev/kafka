---
title: API reference
description: discoverConfigFile, loadConfigFileAsync (and the deprecated loadConfigFileSync), mergeConfigLayers, createDefineConfig
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

`loadConfigFileAsync<T>(path, options?)` is the recommended loader. It loads a resolved path with
dynamic `import()` (`JSON.parse` for `.json`), awaits a sync or async factory export, and handles a
config module that uses top-level `await`.

`T` defaults to `Record<string, unknown>`. Pass `options.assertValid` — an
`(value: unknown) => asserts value is T` function — to validate the resolved value against your
own shape; without one, any plain object is accepted.

```ts
import { discoverConfigFile, loadConfigFileAsync } from '@cookiemonsterdev/kafka-config';

interface AppConfig {
  port?: number;
}

function assertValid(value: unknown): asserts value is AppConfig {
  if (typeof value !== 'object' || value === null) {
    throw new TypeError('app.config: expected an object');
  }
}

const path = discoverConfigFile({ cwd: process.cwd(), name: 'app' });
const config = path == null ? {} : await loadConfigFileAsync<AppConfig>(path, { assertValid });
```

### Deprecated: `loadConfigFileSync`

`loadConfigFileSync<T>(path, options?)` is **deprecated** in favour of `loadConfigFileAsync`, but
keeps working. It loads a path with `require()` (`JSON.parse` for `.json`) and resolves a sync
factory export. Results are memoised per resolved absolute path, so N callers pay the load cost
once per process. A config that needs async work fails: top-level `await` throws
`KafkaConfigRequiresAsyncError`, and an async factory throws a `ConfigFileInvalid` error.
`allowTransformFallback`, `installConfigTransformHooks`, and `areConfigTransformHooksInstalled` are
deprecated with it (see below).

### Bun

On Bun, `require()` and `import()` both load TypeScript natively, so no transform fallback is
needed or installed. `installConfigTransformHooks()` is a no-op that returns `false`. A config that
uses top-level `await` also loads through `loadConfigFileSync` there. Either way, prefer
`loadConfigFileAsync`.

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
validate an already-resolved value — for example, inject it into `loadConfigFileAsync`'s
`assertValid` option.

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

Node's built-in `.ts` support only **strips** types; it does not transform constructs that need
real codegen. A config file (or anything it imports) using a TypeScript `enum`, a relative import
missing its file extension, or `export default` in a `.ts` file whose nearest `package.json`
doesn't declare `"type": "module"`, fails on that default path.

**A TypeScript `enum` (or other non-erasable syntax) is never rescued.** Node 26 has no transform
mode, so the load fails with a `ConfigLoadError` naming the file and the fix: replace the `enum`
with a frozen object or a plain union type. `allowTransformFallback` does not change this.

**`loadConfigFileAsync` has no rescue for the other two cases.** It fails with a `ConfigLoadError`
naming the file, the construct, and the fix: add the `.ts` extension to the import, or rename the
file to `.mts` (or set `"type": "module"`).

The deprecated **synchronous loader** (`loadConfigFileSync`) rescues them by default. It installs
synchronous `require()` hooks (`module.registerHooks` + `stripTypeScriptTypes`) and retries — once
per process, and only when the rescue is actually needed, never on the happy path. The rescue is
never silent: a `config.transform-fallback` warning names the file and the same fix.

Pass `allowTransformFallback: false` (deprecated) to `loadConfigFileSync` for CI: the original
failure surfaces as an error instead, with the same rewritten, fix-naming message, and the hooks
are never installed — **as long as no earlier call in the same process already installed them.**
`module.registerHooks` has no `deregister`, so once any earlier lenient call (the default) rescues
a file, every later call in that process — even one passing `allowTransformFallback: false` — can
silently succeed against a rescuable file too, because `require()` itself now transparently
rescues it. For the guarantee to be airtight, set `allowTransformFallback: false` on every call
from the start of the process; don't mix it with a lenient call against a potentially-rescuable
file earlier in the same run. Or switch to `loadConfigFileAsync`, which never installs hooks.

Fix these constructs rather than relying on the fallback — a rescued config loads through the
sync loader but not through `loadConfigFileAsync` or under `node app.config.ts` directly. Write
`enum`-free, erasable TypeScript:

```ts
// Fails — an enum needs a transform Node does not provide
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
`config.loaded`, `config.multiple-candidates`, `config.transform-fallback`, and
`config.sync-load-deprecated` (raised by `@cookiemonsterdev/kafka-core` when `new Kafka()` loads a
TS/JS config file synchronously).

## Errors

`KafkaConfigError` is raised while discovering, loading, or parsing a config file. `.tag` names
the specific failure (`'ConfigFileNotFound'`, `'ConfigLoadError'`, `'ConfigFileInvalid'`,
`'UnsupportedExtension'`) so callers can branch without parsing `.message`.

`KafkaConfigRequiresAsyncError` is raised when a config file (or something it imports) uses
top-level `await`, which the deprecated `loadConfigFileSync` cannot handle on Node — use
`loadConfigFileAsync` instead. (On Bun, `require()` supports top-level `await`, so it isn't raised.)

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
