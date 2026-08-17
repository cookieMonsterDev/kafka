<p align="center">
  <img src="assets/brand/logo-256.png" width="160" height="160" alt="kafka">
</p>

<h1 align="center">kafka</h1>

<p align="center">
  A TypeScript Apache Kafka client for Node.js.<br />
  Brokers from <strong>Kafka 0.10</strong> onward · producer, consumer, and admin
</p>

<p align="center">
  <a href="https://github.com/cookieMonsterDev/kafka/actions/workflows/ci.yml"><img src="https://github.com/cookieMonsterDev/kafka/actions/workflows/ci.yml/badge.svg?branch=develop" alt="CI" /></a>
  <a href="https://www.npmjs.com/package/@kafka/core"><img src="https://img.shields.io/npm/v/@kafka/core.svg" alt="npm" /></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT License" /></a>
  <a href=".nvmrc"><img src="https://img.shields.io/badge/node-%3E%3D24-brightgreen.svg" alt="Node.js 24+" /></a>
  <a href="package.json"><img src="https://img.shields.io/badge/pnpm-11-F69220.svg" alt="pnpm 11" /></a>
</p>

`@kafka/core` speaks the Kafka protocol directly: it negotiates API versions with the broker, uses `bigint` for offsets, and ships TypeScript types from source. `@kafka/docs` is the documentation site for that client.

Install the client from npm. The workspace root is private and is not published.

```sh
npm install @kafka/core
```

Docs: [https://cookiemonsterdev.github.io/kafka/](https://cookiemonsterdev.github.io/kafka/). Local site: `pnpm --filter @kafka/docs... build && pnpm --filter @kafka/docs dev`.

## Features

- **Producer** — `send` / `sendBatch`, optional idempotence, transactions, headers, GZIP and ZSTD (Snappy and LZ4 are pluggable)
- **Consumer groups** — pause, resume, seek, `eachMessage` / `eachBatch`, `consumer.stream()`, classic group protocol
- **Partition assigners** — range, round-robin (default), sticky, cooperative-sticky
- **Admin** — topics, configs, ACLs, offsets, groups, SCRAM credentials, leader election
- **Security** — SSL/TLS, SASL PLAIN, SCRAM-SHA-256/512, OAUTHBEARER, AWS IAM helper
- **Compatibility** — Kafka **0.10+** via `ApiVersions` (not Java-client 4.x parity)
- **TypeScript-first** — generated `.d.ts`, `AbortSignal` on connect/send/run, `await using` via `Symbol.asyncDispose`

## Quick start

```ts
import { Kafka, CompressionTypes, logLevel } from '@kafka/core';

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

const consumer = kafka.consumer({ groupId: 'my-group' });
await consumer.connect();
await consumer.subscribe({ topics: ['events'], fromBeginning: true });
await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    console.log({ topic, partition, offset: message.offset, value: message.value?.toString() });
  },
});
```

Offsets are `bigint` (`42n`), not strings. More examples: [Getting started](packages/docs/src/content/docs/start/getting-started.md), [public API](packages/docs/src/content/docs/reference/public-api.md), [compatibility](packages/docs/src/content/docs/reference/compatibility.md).

Run the site locally after `pnpm install`:

```sh
pnpm --filter @kafka/docs... build
pnpm --filter @kafka/docs dev   # http://localhost:4321
```

## Packages

| Package       | Path            | What it is                                    |
| ------------- | --------------- | --------------------------------------------- |
| `@kafka/core` | `packages/core` | TypeScript Kafka client (Kafka 0.10+), on npm |
| `@kafka/docs` | `packages/docs` | Astro documentation site (GitHub Pages)       |

## Requirements

- **Node.js 24** (Krypton LTS), pinned in `.nvmrc`. A wrong version fails `pnpm install`.
- **pnpm 11**, pinned via `packageManager`.

```sh
nvm use
corepack enable
pnpm install
```

## Development

```sh
pnpm dev           # docs + library watchers
pnpm build         # all packages, dependency order
pnpm lint          # ESLint
pnpm format:check  # Prettier
pnpm typecheck     # tsc --noEmit + astro check
pnpm test          # unit tests only (never starts Docker)
```

```sh
pnpm --filter @kafka/core test
KAFKA_VERSION=0.10 pnpm --filter @kafka/core test:integration
KAFKA_VERSION=4.0 pnpm --filter @kafka/core test:integration
```

Branch names, Conventional Commits, PR flow, code style, and how to add a package: **[CONTRIBUTING.md](CONTRIBUTING.md)**. Releases and GitHub settings: **[`.github/branch-setup.md`](.github/branch-setup.md)**. Per-package details: [`@kafka/core`](packages/core/README.md), [`@kafka/docs`](packages/docs/README.md). Integration clusters: [`packages/core/test/assets/README.md`](packages/core/test/assets/README.md).

## License

[MIT](LICENSE) © Mykhailo Toporkov

Apache Kafka and Kafka are trademarks of the Apache Software Foundation. This project is not affiliated with, endorsed by, or sponsored by the ASF.
