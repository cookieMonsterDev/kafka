---
title: Configuration
description: Share KafkaConfig across producer, consumer, and admin with a kafka.config.ts file
order: 9
section: guides
---

Passing `brokers` (and everything else) to every `new Kafka({...})` call works, but a monorepo
with several entry points — a web server, a worker, a CLI script — usually wants one shared
source of truth instead. `new Kafka()` fills in whatever a call omits from a `kafka.config.ts`
file:

```ts
// kafka.config.ts, at the repo root or a workspace root
import { defineConfig } from '@cookiemonsterdev/kafka-core';

export default defineConfig({
  client: {
    brokers: ['localhost:9092'],
  },
});
```

```ts
// anywhere under that directory
import { Kafka } from '@cookiemonsterdev/kafka-core';

const kafka = new Kafka(); // brokers comes from the file
```

Discovery walks upward from the current directory and stops at the nearest `.git`,
`pnpm-workspace.yaml`, or workspace `package.json` — so `pnpm --filter ./apps/worker start` still
finds a `kafka.config.ts` at the monorepo root. It only runs when a call omits `brokers`, so
existing code that already passes `brokers` never gains a filesystem read. See
[Config file](../../reference/config-file/) for the full precedence and discovery rules.

## Per-environment configs

There is no built-in `env:`/profile concept — a config file is plain TypeScript, so branch on
`process.env` directly:

```ts
import { defineConfig } from '@cookiemonsterdev/kafka-core';

const configs = {
  development: { client: { brokers: ['localhost:9092'] } },
  production: { client: { brokers: ['broker-1:9092', 'broker-2:9092', 'broker-3:9092'], ssl: true } },
};

export default defineConfig(configs[process.env.NODE_ENV ?? 'development']);
```

## Sharing defaults across producer, consumer, and admin

`producer`, `consumer`, `shareConsumer`, and `admin` sections apply the same way, under whatever
each call passes directly:

```ts
export default defineConfig({
  client: { brokers: ['localhost:9092'] },
  producer: { lingerMs: 20, compression: 'gzip' },
  consumer: { sessionTimeout: 45_000 },
});
```

```ts
kafka.producer(); // lingerMs 20, compression gzip
kafka.producer({ lingerMs: 0 }); // this call's lingerMs wins; compression gzip still applies
```

## Debugging which config file loaded

```ts
kafka.configSource();
// { path: '/repo/kafka.config.ts', keys: { brokers: 'file', clientId: 'default', ... } }
```

`path` is `null` when no config file was used at all — useful for confirming a client that should
be reading `brokers` explicitly (in CI, say) never picked up a stray config file from an
unexpected parent directory.

## Top-level `await`, or an async factory

The synchronous constructor cannot load a config file that needs async work. Use
`Kafka.fromConfig()` instead — see [Config file](../../reference/config-file/#kafkafromconfig--kafkafrom).
