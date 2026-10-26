---
name: kafka-core-setup
description: >
  Use when installing @cookiemonsterdev/kafka-core, creating the Kafka client, pointing it at
  brokers, enabling TLS or SASL (PLAIN, SCRAM, OAUTHBEARER, GSSAPI/Kerberos, delegation tokens),
  sharing connection settings through a kafka.config.ts file or KAFKA_* environment variables, or
  debugging which brokers and credentials a client actually resolved.
metadata:
  purpose: >-
    Gets a project from no Kafka dependency to a connected, correctly secured client, with
    connection settings defined once and shared across producers, consumers, and admins.
  type: lifecycle
  library: '@cookiemonsterdev/kafka-core'
  library_version: '2.5.1'
requires:
  - kafka-core
sources:
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/core/start/installation.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/core/guides/configuration.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/core/guides/security.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/core/reference/config-file.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/core/reference/environment-variables.md'
---

# kafka-core setup

This skill builds on `kafka-core`. Read it first for the rules every task shares.

## Setup

Requires Node.js 24+ (ZSTD uses `zlib.zstd*`) or Bun 1.4+. Install with the project's package
manager:

```sh
npm install @cookiemonsterdev/kafka-core
```

Minimal client:

```ts
import { Kafka, logLevel } from '@cookiemonsterdev/kafka-core';

export const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092'],
  logLevel: logLevel.INFO,
});
```

Create this once (for example in `src/kafka.ts`) and import it wherever a producer, consumer, or
admin is needed.

## Core Patterns

### Share settings with a config file

For several entry points (server, worker, scripts), put connection settings in one
`kafka.config.ts` at the repo or workspace root and load it with `Kafka.fromConfig()`:

```ts
// kafka.config.ts
import { defineConfig } from '@cookiemonsterdev/kafka-core';

const configs = {
  development: { client: { brokers: ['localhost:9092'] } },
  production: { client: { brokers: ['broker-1:9092', 'broker-2:9092', 'broker-3:9092'], ssl: true } },
};

export default defineConfig(configs[process.env.NODE_ENV === 'production' ? 'production' : 'development']);
```

```ts
// anywhere below that directory
import { Kafka } from '@cookiemonsterdev/kafka-core';

const kafka = await Kafka.fromConfig();
```

Discovery walks upward from the current directory and stops at the nearest `.git`,
`pnpm-workspace.yaml`, or workspace `package.json`. It only runs when the call omits `brokers`.
The file may also hold `producer`, `consumer`, `shareConsumer`, and `admin` sections; values
passed directly to `kafka.producer({...})` and similar calls win over the file.

Use `await Kafka.fromConfig()` rather than `new Kafka()` for a TS/JS config file: the synchronous
constructor loads it through a deprecated path and warns. A `kafka.config.json` file loads
synchronously without a warning.

### Read settings from environment variables

`fromEnv(env)` maps a fixed list of `KAFKA_*` variables (`KAFKA_BROKERS`, `KAFKA_CLIENT_ID`,
`KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`, `KAFKA_SSL`,
`KAFKA_SSL_CA_FILE`, `KAFKA_SSL_CERT_FILE`, `KAFKA_SSL_KEY_FILE`, …) to a partial config:

```ts
import { defineConfig, fromEnv } from '@cookiemonsterdev/kafka-core';

export default defineConfig({
  client: fromEnv(process.env),
});
```

A variable that cannot be parsed is reported as a warning and left out; it never becomes `NaN`.
Only `plain`, `scram-sha-256`, and `scram-sha-512` can be built from environment variables.

### TLS and SASL

```ts
import { readFileSync } from 'node:fs';
import { Kafka } from '@cookiemonsterdev/kafka-core';

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['broker.example.com:9093'],
  ssl: { ca: [readFileSync('/etc/kafka/ca.pem', 'utf8')] },
  sasl: { mechanism: 'scram-sha-512', username: 'alice', password: process.env.KAFKA_PASSWORD ?? '' },
});
```

`ssl: true` uses Node's TLS defaults; pass a `tls.ConnectionOptions` object for custom CAs or
client certificates. Mechanisms: `plain`, `scram-sha-256`, `scram-sha-512`, `oauthbearer` (token
provider), `gssapi` (Kerberos), `aws`, or a custom `{ mechanism, authenticationProvider }`.

For GSSAPI, read [GSSAPI and delegation tokens](references/gssapi-and-tokens.md) before writing
the config.

### Check what was resolved

```ts
console.log(kafka.configSource());
// { path: '/repo/kafka.config.ts', keys: { brokers: 'file', clientId: 'default', ... } }
```

`path` is `null` when no config file was used.

## Common Mistakes

- **CRITICAL — Hard-coding credentials in source or in a committed `kafka.config.ts`.** Read
  secrets from the environment (`process.env`, `fromEnv`) or a secret store.
- **HIGH — `sasl` without `ssl` against a public broker.** PLAIN sends the password in clear text
  unless TLS is on. Set `ssl` whenever the broker listener is `SASL_SSL`.
- **HIGH — Retrying `KafkaSASLAuthenticationError`.** It is non-retriable: wrong credentials,
  wrong mechanism, or the broker does not enable that mechanism. Source: core guides/security.
- **MEDIUM — A stray config file from a parent directory.** Discovery walks upward. In CI, pass
  `brokers` explicitly and assert `kafka.configSource().path === null` if no file should apply.
- **MEDIUM — Older Node.js.** The package declares `engines.node >= 24` and relies on
  `zlib.zstd*`. Upgrade Node rather than pinning an older client.

## Completion

- `await kafka.admin().connect()` (or a producer `connect()`) resolves without error.
- `kafka.configSource()` shows the expected file path and the expected source for `brokers`.
- No password, token, or key appears in committed files.

On failure: `ECONNREFUSED` means the broker address or port is wrong; a TLS error means the
`ssl` options do not match the listener; `KafkaSASLAuthenticationError` means credentials or
mechanism are wrong.

## References

- [GSSAPI and delegation tokens](references/gssapi-and-tokens.md): read when configuring Kerberos
  or logging in with an Admin-minted delegation token.
- Config file precedence and discovery rules:
  https://cookiemonsterdev.github.io/kafka/docs/core/reference/config-file/
