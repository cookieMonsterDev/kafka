---
title: Config file
description: kafka.config.* discovery, precedence, Kafka.fromConfig, and Kafka.from
order: 9
section: reference
---

`new Kafka()` can fill in whatever options a call omits from a `kafka.config.ts` (or `.mts`,
`.cts`, `.js`, `.mjs`, `.cjs`, `.json`) file, discovered by walking up from the current directory.
The loader itself lives in the standalone, zero-dependency
[`@cookiemonsterdev/kafka-config`](https://www.npmjs.com/package/@cookiemonsterdev/kafka-config)
package; this page covers the Kafka-typed facade `@cookiemonsterdev/kafka-core` builds on top of
it.

## Precedence

For every `KafkaConfig` key, the highest layer where the value is defined wins:

1. **The argument passed to the call** (`new Kafka({...})`, `producer({...})`, `consumer({...})`,
   `shareConsumer({...})`, `admin({...})`).
2. **The matching section of the config file** — `client` for the constructor, `producer` /
   `consumer` / `shareConsumer` / `admin` for each factory method.
3. **The constructor's / factory method's own default.**

A key set nowhere is left out of the merged object entirely, so a method's own default (`lingerMs
= 5`, `enforceRequestTimeout = true`, ...) fires exactly as if the config file did not exist.
`undefined` always means "absent, fall through to the next layer" — never "explicitly unset to
falsy". `logLevel: 0`, `enforceRequestTimeout: false`, and `clientId: ''` all survive the merge.

`retry` is merged one level deep (an explicit `{ retries: 9 }` keeps a file's `maxRetryTime`);
`sasl`, `ssl`, `brokers`, `metrics`, `socketFactory`, and `logCreator` are replaced atomically —
merging two SASL mechanisms, for instance, would produce an invalid object.

One pre-existing exception to this table: `KAFKA_LOG_LEVEL`, read by the logger module directly,
overrides an explicitly passed `logLevel` regardless of any of the above. This predates the config
file and is not extended by it — it is called out here only so a surprising `logLevel` is never
blamed on the config file.

## Discovery

- Candidate ladder, checked in this order: `kafka.config.ts` → `.mts` → `.cts` → `.js` → `.mjs` →
  `.cjs` → `.json`, then the same ladder under `.config/kafka.*`.
- Search starts at the current directory and walks upward, stopping at the first directory
  containing `.git`, `pnpm-workspace.yaml`, or a `package.json` with a `workspaces` field.
- The first directory that has any candidate wins entirely — a config a level up is never merged
  with one closer to the current directory.
- Two candidates in the same directory: the first one in ladder order wins, and a warning names
  both, so the ambiguity is never silent.

### The `config` option

```ts
new Kafka({
  // brokers omitted — discovers only because of that
});

new Kafka({ brokers: ['localhost:9092'] });
// brokers given — no filesystem read at all, matching every existing call

new Kafka({ brokers: ['localhost:9092'], config: true });
// discovers anyway, and the file can still override anything this call didn't set

new Kafka({ config: false });
// never discovers, even with no brokers — throws if nothing else supplies one

new Kafka({ config: './config/prod.kafka.ts' });
// an explicit path, resolved against cwd — a missing path is a hard error, never a silent fallback
```

## The config file itself

```ts
// kafka.config.ts
import { defineConfig } from '@cookiemonsterdev/kafka-core';

export default defineConfig({
  client: {
    brokers: ['localhost:9092'],
  },
  producer: {
    lingerMs: 20,
  },
  consumer: {
    sessionTimeout: 45_000,
  },
});
```

`defineConfig` is an identity helper: it freezes the object and validates that each known section
(`client`, `producer`, `consumer`, `shareConsumer`, `admin`), when present, is itself an object. An
unrecognized top-level key passes through unvalidated, so an older `@cookiemonsterdev/kafka-core`
never rejects a file written for a newer consumer of the same file. A bare object, or a sync/async
factory function, are accepted too — `defineConfig` is documentation, not a requirement.

`client` mirrors `KafkaConfig` (minus `config`, which only means something as a call argument).
`consumer` and `shareConsumer` make `groupId` optional even where the runtime call requires it, so
a file can supply shared consumer defaults without hardcoding one group.

A config file that needs top-level `await`, or that exports an async factory, cannot be loaded by
the synchronous constructor — use [`Kafka.fromConfig()`](#kafkafromconfig--kafkafrom) instead. A
`.ts` file relying on a construct the default strip-only loader cannot handle (a TypeScript
`enum`, an extensionless relative import) is rescued through a one-time transform fallback, with a
warning on stderr naming the file and the fix.

## `Kafka.fromConfig` / `Kafka.from`

```ts
// async — the only path for top-level await or an async factory export
const kafka = await Kafka.fromConfig({ clientId: 'my-app' }, { cwd: import.meta.dirname });

// synchronous — for a caller that already loaded the file itself and wants to
// construct several clients from it without discovering or reading it again
import { loadKafkaConfig } from '@cookiemonsterdev/kafka-core';

const fileConfig = loadKafkaConfig('./kafka.config.ts');
const kafka = Kafka.from(fileConfig, { clientId: 'my-app' });
```

`Kafka.fromConfig()` shares `new Kafka()`'s discovery; every entry point — the constructor,
`fromConfig`, and `from` — shares the same merge logic, so the three cannot drift on how a config
file's values are applied. `Kafka.from()` alone does no discovery, by design: it exists precisely
for a caller that already has a loaded `KafkaFileConfig` and wants to skip that step.

Bundled or serverless deployments (Vite, webpack, Lambda) should prefer `Kafka.from()` with an
inlined config object over letting `new Kafka()` discover a file at runtime — `new Kafka()` and
`Kafka.fromConfig()` both resolve a config file path dynamically, which a bundler cannot analyze
statically the way it can a plain import.

## `kafka.configSource()`

Reports where each `KafkaConfig` key's value came from — the call, the config file, or neither:

```ts
const kafka = new Kafka({ clientId: 'my-app' });

kafka.configSource();
// {
//   path: '/repo/kafka.config.ts' | null,
//   keys: { brokers: 'file', clientId: 'explicit', connectionTimeout: 'default', ... }
// }
```

The report carries provenance only, never a value, so there is nothing in it that needs redacting
regardless of which key — including `sasl` — it names.

## See also

- [Configuration reference](../configuration/) for every `KafkaConfig` / `ProducerConfig` /
  `ConsumerConfig` / `ShareConsumerConfig` / `AdminConfig` field and its default.
- [Configuration guide](../../guides/configuration/) for a walkthrough of setting up a config file.
