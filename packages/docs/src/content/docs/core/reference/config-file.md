---
title: Config file
description: kafka.config.* discovery, defineConfig, and loadKafkaConfig
order: 9
section: reference
---

`@cookiemonsterdev/kafka-core/config` — a separate subpath, not re-exported from the package
root — is this package's Kafka-typed facade on top of
[`@cookiemonsterdev/kafka-config`](../../../config/start/introduction/), the generic config-file
loader: `KafkaFileConfig`, a bound `defineConfig`, and `loadKafkaConfig`. See that package's docs
for discovery, loading, merging, and the transform-hook rescue in more depth — this page covers
the Kafka-specific layer on top.

It is a standalone loader today: nothing in this package reads a config file automatically yet.
`new Kafka()` picking up a config file is tracked separately; this page documents the facade
itself.

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

## Loading

`loadKafkaConfig(options?)` discovers (unless an explicit `path` or `KAFKA_CONFIG` is set), loads,
and **never throws** — every failure comes back as `{ ok: false, error }`, with `error.tag` one of
`'ConfigFileNotFound'`, `'ConfigLoadError'`, `'ConfigFileInvalid'`, or `'UnsupportedExtension'`. A
successful `config` is always a frozen plain object, even when nothing was found (`{}`).

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

By default, `loadKafkaConfig` rescues all three cases: it installs synchronous `require()` hooks
and retries — once per process, and only when the rescue is actually needed, never on the happy
path. The rescue is never silent: a warning names the file and the exact fix (replace the `enum`
with a frozen object; add the `.ts` extension). Pass `allowTransformFallback: false` for CI: the
original failure surfaces as an error instead. See
[the loader's docs](../../../config/reference/api/#the-erasable-typescript-constraint) for the
full detail, including a known limitation around mixing lenient and strict calls in one process.

## Diagnostics

`loadKafkaConfig` accepts an `onDiagnostic` callback: `{ code, level: 'info' | 'warn', message,
path?, ...extra }`. The default handler writes only `'warn'`-level diagnostics to stderr, prefixed
`[kafka-config]`; `'info'` diagnostics (`config.loaded`, and `config.multiple-candidates` when
it's not also escalated) are silent unless you supply your own callback.

## What this does **not** do

- **No automatic env reading.** `loadKafkaConfig` never touches `process.env` except for the
  single, explicit `KAFKA_CONFIG` override described above. (The pre-existing `KAFKA_LOG_LEVEL`
  override in `loggers/index.ts` is a separate, older, documented wart — not a precedent this
  facade extends.)
- **No remote or inherited config.** No remote `extends`, no config inheritance chains, no
  YAML/TOML config.
- **No secret redaction here.** `loadKafkaConfig` returns whatever the config file exports,
  verbatim; redacting secrets before printing them is the caller's job.
