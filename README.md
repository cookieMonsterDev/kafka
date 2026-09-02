<p align="center">
  <img src="packages/docs/public/logo-icon.svg" width="96" height="96" alt="kafka">
</p>

<h1 align="center">kafka</h1>

<p align="center">
  A TypeScript Apache Kafka client for Node.js.<br />
  Brokers from <strong>Kafka 0.10</strong> onward · producer, consumer, and admin
</p>

<p align="center">
  <a href="https://github.com/cookieMonsterDev/kafka/actions/workflows/ci.yml"><img src="https://github.com/cookieMonsterDev/kafka/actions/workflows/ci.yml/badge.svg?branch=develop" alt="CI" /></a>
  <a href="https://www.npmjs.com/package/@cookiemonsterdev/kafka-core"><img src="https://img.shields.io/npm/v/@cookiemonsterdev/kafka-core.svg" alt="npm" /></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT License" /></a>
  <a href=".nvmrc"><img src="https://img.shields.io/badge/node-%3E%3D24-brightgreen.svg" alt="Node.js 24+" /></a>
  <a href="package.json"><img src="https://img.shields.io/badge/pnpm-11-F69220.svg" alt="pnpm 11" /></a>
</p>

This repo is a small family of packages built around one Kafka client for Node.js:

- **[`kafka-core`](packages/core)** — the client itself: producer, consumer, and admin, talking directly to the Kafka wire protocol (no Java client, no native bindings).
- **[`kafka-cli`](packages/cli)** — a `kafka` command you run from a terminal, built on top of `kafka-core`.
- **[`kafka-config`](packages/config)** — the generic `kafka.config.*` file loader both of the above use to read settings from disk.
- **[`kafka-docs`](packages/docs)** — the documentation site you're one click away from below.

Under the hood, `kafka-core` negotiates API versions with the broker itself, uses `bigint` for offsets (Kafka offsets can exceed what JavaScript's `number` can represent exactly), and ships TypeScript types generated from source rather than hand-maintained.

Install the client from npm — the workspace root itself is private and is not published.

```sh
npm install @cookiemonsterdev/kafka-core
```

Docs: [https://cookiemonsterdev.github.io/kafka/](https://cookiemonsterdev.github.io/kafka/). Local site: `pnpm --filter @cookiemonsterdev/kafka-docs... build && pnpm --filter @cookiemonsterdev/kafka-docs dev`.

## Features

- **Producer** — `send` / `sendBatch`, optional idempotence, transactions, headers, GZIP, Snappy, LZ4, and ZSTD
- **Consumer groups** — pause, resume, seek, `eachMessage` / `eachBatch`, `consumer.stream()`, classic protocol, opt-in KIP-848
- **Share groups** — `kafka.shareConsumer()` (KIP-932) on Kafka 4.1+
- **Partition assigners** — range, round-robin (default), sticky, cooperative-sticky
- **Admin** — topics, configs, ACLs, offsets, groups, share groups, SCRAM, transactions, KRaft
- **Security** — SSL/TLS, SASL PLAIN, SCRAM-SHA-256/512, OAUTHBEARER, GSSAPI / Kerberos, AWS IAM helper
- **Compatibility** — Kafka **0.10+** via `ApiVersions` (not Java-client 4.x parity)
- **TypeScript-first** — generated `.d.ts`, `AbortSignal` on connect/send/run, `await using` via `Symbol.asyncDispose`

## Quick start

```ts
import { Kafka, CompressionTypes, logLevel } from '@cookiemonsterdev/kafka-core';

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

Offsets are `bigint` (`42n`), not strings. More examples: [Getting started](packages/docs/src/content/docs/core/start/getting-started.md), [public API](packages/docs/src/content/docs/core/reference/public-api.md), [compatibility](packages/docs/src/content/docs/core/reference/compatibility.md).

Run the site locally after `pnpm install`:

```sh
pnpm --filter @cookiemonsterdev/kafka-docs... build
pnpm --filter @cookiemonsterdev/kafka-docs dev   # http://localhost:4321
```

## Packages

Versions below are live badges pulled from the npm registry, so they always match what
`npm install` would actually give you.

| Package                                                       | Version                                                                                                                                     | What it is                                                                          |
| ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| [`@cookiemonsterdev/kafka-core`](packages/core/README.md)     | [![npm](https://img.shields.io/npm/v/%40cookiemonsterdev%2Fkafka-core.svg)](https://www.npmjs.com/package/@cookiemonsterdev/kafka-core)     | TypeScript Kafka client (Kafka 0.10+)                                               |
| [`@cookiemonsterdev/kafka-config`](packages/config/README.md) | [![npm](https://img.shields.io/npm/v/%40cookiemonsterdev%2Fkafka-config.svg)](https://www.npmjs.com/package/@cookiemonsterdev/kafka-config) | Generic `kafka.config.*` file loader                                                |
| [`@cookiemonsterdev/kafka-cli`](packages/cli/README.md)       | [![npm](https://img.shields.io/npm/v/%40cookiemonsterdev%2Fkafka-cli.svg)](https://www.npmjs.com/package/@cookiemonsterdev/kafka-cli)       | Command-line admin client — topics, ping, and a passthrough for the rest of `Admin` |
| [`@cookiemonsterdev/kafka-docs`](packages/docs/README.md)     | `private`, not published                                                                                                                    | Astro documentation site (GitHub Pages)                                             |

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
pnpm --filter @cookiemonsterdev/kafka-core test
KAFKA_VERSION=0.10 pnpm --filter @cookiemonsterdev/kafka-core test:integration
KAFKA_VERSION=4.0 pnpm --filter @cookiemonsterdev/kafka-core test:integration
```

Want to contribute? Branch names, Conventional Commits, PR flow, code style, releasing, and how to
add a package all live in **[CONTRIBUTING.md](CONTRIBUTING.md)**. Setting up an integration
cluster locally: [`packages/core/test/assets/README.md`](packages/core/test/assets/README.md).

## License

[MIT](LICENSE) © Mykhailo Toporkov

Apache Kafka and Kafka are trademarks of the Apache Software Foundation. This project is not affiliated with, endorsed by, or sponsored by the ASF.
