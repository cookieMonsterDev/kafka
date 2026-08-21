# @cookiemonsterdev/kafka-core

TypeScript Apache Kafka client for **Kafka 0.10+**. Protocol versions are negotiated from `ApiVersions`. Offsets are `bigint`. Types are generated from source.

This package is the library in the [kafka](https://github.com/cookieMonsterDev/kafka) workspace. It is published to npm as [`@cookiemonsterdev/kafka-core`](https://www.npmjs.com/package/@cookiemonsterdev/kafka-core).

<p>
  <a href="https://github.com/cookieMonsterDev/kafka/actions/workflows/ci.yml"><img src="https://github.com/cookieMonsterDev/kafka/actions/workflows/ci.yml/badge.svg?branch=develop" alt="CI" /></a>
  <a href="https://www.npmjs.com/package/@cookiemonsterdev/kafka-core"><img src="https://img.shields.io/npm/v/@cookiemonsterdev/kafka-core.svg" alt="npm" /></a>
  <a href="../../LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT License" /></a>
</p>

## Features

| Area        | What you get                                                                    |
| ----------- | ------------------------------------------------------------------------------- |
| Producer    | `send` / `sendBatch`, headers, optional idempotence, transactions, linger/batch |
| Consumer    | Groups with pause/resume/seek, `run()`, `stream()`, classic protocol            |
| Assigners   | Range, round-robin (default), sticky, cooperative-sticky                        |
| Admin       | Topics, configs, ACLs, offsets, groups, SCRAM, leader election                  |
| Compression | GZIP and ZSTD built in; Snappy and LZ4 via `CompressionCodecs`                  |
| Security    | SSL/TLS, SASL PLAIN / SCRAM / OAUTHBEARER / GSSAPI, AWS IAM helper              |
| DX          | `AbortSignal`, `await using` (`Symbol.asyncDispose`), generated `.d.ts`         |

Not in scope: Kafka Streams, Kafka Connect, Java-client 4.x parity. See [compatibility](../docs/src/content/docs/core/reference/compatibility.md).

## Usage

From another workspace package, or after `pnpm --filter @cookiemonsterdev/kafka-core build`:

```ts
import { Kafka, CompressionTypes, logLevel, Partitioners } from '@cookiemonsterdev/kafka-core';

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092'],
  logLevel: logLevel.INFO,
});

const producer = kafka.producer();
await producer.connect();
await producer.send({
  topic: 'events',
  compression: CompressionTypes.GZIP,
  messages: [{ key: 'user-1', value: 'hello' }],
});
await producer.disconnect();

const consumer = kafka.consumer({ groupId: 'my-group' });
await consumer.connect();
await consumer.subscribe({ topics: ['events'], fromBeginning: true });
await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    console.log({
      topic,
      partition,
      offset: message.offset, // bigint
      value: message.value?.toString(),
    });
  },
});
```

The `exports` field points at `dist/`, so **`dist/` must exist** before a dependent package builds. `pnpm -r` does that automatically. After a `clean`, build this package first:

```sh
pnpm --filter @cookiemonsterdev/kafka-docs... build   # "..." includes dependencies
```

SASL/GSSAPI (Kerberos) is opt-in. Install the optional `kerberos` package if you are not supplying `sasl.gssProvider`:

```sh
npm install kerberos
```

See [Security](../docs/src/content/docs/core/guides/security.md). CI does not run a KDC.

`await using` works because producer, consumer, and admin implement `Symbol.asyncDispose` (it calls `disconnect()`).

To keep pre-2.0 key routing, pass `createPartitioner: Partitioners.LegacyPartitioner`. The default is murmur2 (`Partitioners.DefaultPartitioner`), not the Java 4.x sticky partitioner.

## Documentation

| Page                                                                            | Contents                                         |
| ------------------------------------------------------------------------------- | ------------------------------------------------ |
| [Introduction](../docs/src/content/docs/core/start/introduction.md)             | What the client is and which brokers it talks to |
| [Getting started](../docs/src/content/docs/core/start/getting-started.md)       | Produce and consume                              |
| [Producer API](../docs/src/content/docs/core/reference/producer.md)             | `send`, `Message`, `RecordMetadata`              |
| [Consumer API](../docs/src/content/docs/core/reference/consumer.md)             | `run`, `stream`, `KafkaMessage`                  |
| [Errors](../docs/src/content/docs/core/reference/errors.md)                     | Public classes and protocol codes                |
| [Security](../docs/src/content/docs/core/guides/security.md)                    | TLS and SASL, including GSSAPI                   |
| [Compatibility](../docs/src/content/docs/core/reference/compatibility.md)       | Defaults vs the Java client, missing APIs        |
| [Breaking changes](../docs/src/content/docs/core/migration/breaking-changes.md) | Offsets, MessageSet, ZSTD, env vars              |

Local site: `pnpm --filter @cookiemonsterdev/kafka-docs dev` → <http://localhost:4321>

## Local development

From the repo root (after `pnpm install`):

```sh
pnpm --filter @cookiemonsterdev/kafka-core dev        # vite build --watch
pnpm --filter @cookiemonsterdev/kafka-core build      # JS + .d.ts into dist/
pnpm --filter @cookiemonsterdev/kafka-core typecheck  # src + tests, tsc --noEmit
pnpm --filter @cookiemonsterdev/kafka-core clean      # remove dist/
```

Or from this directory:

```sh
cd packages/core
pnpm dev
```

### Layout

```
src/index.ts   public barrel — everything public is exported here
src/client.ts  Kafka class (producer / consumer / admin)
dist/          build output (git-ignored)
tsconfig.json  extends ../../tsconfig.base.json
```

Compiler options are shared: strict mode, `bundler` module resolution, `erasableSyntaxOnly` (no enums or parameter properties). Override per-package settings in `tsconfig.json`, not in the base.

### Adding a dependency

```sh
pnpm --filter @cookiemonsterdev/kafka-core add <pkg>
pnpm --filter @cookiemonsterdev/kafka-core add -D <pkg>
```

For a version shared with other packages, add it to the `catalog:` in the root `pnpm-workspace.yaml` and reference it as `"<pkg>": "catalog:"`.

## Tests

Unit tests are protocol fixtures and do not start Docker:

```sh
pnpm --filter @cookiemonsterdev/kafka-core test
```

Integration tests select a compose file from `KAFKA_VERSION` (default `4.0`). You do not edit compose paths:

```sh
KAFKA_VERSION=0.10 pnpm --filter @cookiemonsterdev/kafka-core test:integration
KAFKA_VERSION=4.0 pnpm --filter @cookiemonsterdev/kafka-core test:integration
KAFKA_VERSION=4.3 pnpm --filter @cookiemonsterdev/kafka-core test:integration
```

`KAFKA_EXTERNAL=1` skips compose up/down. `DO_NOT_STOP=1` leaves the cluster running after the suite. Mapping, feature gates, and CI matrix: [`test/assets/README.md`](test/assets/README.md).

## Contributing

[CONTRIBUTING.md](../../CONTRIBUTING.md) — branch names, Conventional Commits, PR flow, and code style.

## License

[MIT](../../LICENSE) © Mykhailo Toporkov
