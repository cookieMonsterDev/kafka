---
title: Environment variables and secrets
description: fromEnv, the KAFKA_* allow-list, and redactKafkaConfig
order: 10
section: reference
---

`@cookiemonsterdev/kafka-core` never reads `process.env` on its own. There is one pre-existing
exception, and it is a wart, not a precedent: the logger module reads `KAFKA_LOG_LEVEL` directly and
uses it to override an explicitly passed `logLevel` (see the
[config file precedence table](../config-file/#precedence)). Nothing else in this client touches
the environment implicitly. Everything below is opt-in — you call it yourself, typically from a
`kafka.config.ts` file.

## `fromEnv`

```ts
// kafka.config.ts
import { defineConfig, fromEnv } from '@cookiemonsterdev/kafka-core';

export default defineConfig({
  client: fromEnv(process.env),
});
```

`fromEnv(env, options?)` reads a fixed allow-list of `KAFKA_*` variables into a `Partial<KafkaConfig>`.
It takes `env` as a parameter rather than reading `process.env` itself, so it never needs the
environment mutated in a test and never depends on when it happens to be called.

| Variable                        | `KafkaConfig` field                                                                     |
| ------------------------------- | --------------------------------------------------------------------------------------- |
| `KAFKA_BROKERS`                 | `brokers` — comma-separated, trimmed, empty entries dropped                             |
| `KAFKA_CLIENT_ID`               | `clientId`                                                                              |
| `KAFKA_SASL_MECHANISM`          | `sasl.mechanism` — only `plain`, `scram-sha-256`, `scram-sha-512`                       |
| `KAFKA_SASL_USERNAME`           | `sasl.username`                                                                         |
| `KAFKA_SASL_PASSWORD`           | `sasl.password`                                                                         |
| `KAFKA_SSL`                     | `ssl` (boolean) — ignored once any `_FILE` or `_REJECT_UNAUTHORIZED` variable is set    |
| `KAFKA_SSL_CA_FILE`             | `ssl.ca` — read from the given path                                                     |
| `KAFKA_SSL_CERT_FILE`           | `ssl.cert` — read from the given path                                                   |
| `KAFKA_SSL_KEY_FILE`            | `ssl.key` — read from the given path                                                    |
| `KAFKA_SSL_REJECT_UNAUTHORIZED` | `ssl.rejectUnauthorized` (boolean)                                                      |
| `KAFKA_CONNECTION_TIMEOUT`      | `connectionTimeout`                                                                     |
| `KAFKA_REQUEST_TIMEOUT`         | `requestTimeout`                                                                        |
| `KAFKA_LOG_LEVEL`               | `logLevel` — matched case-insensitively against `NOTHING`/`ERROR`/`WARN`/`INFO`/`DEBUG` |

`KAFKA_PROFILE`, `KAFKA_CONFIG`, and `KAFKA_OUTPUT` are not on this list — they are CLI concepts with
no corresponding `KafkaConfig` field, so `fromEnv` has no opinion about them; a CLI built on this
client reads them directly.

A variable that cannot be parsed — `KAFKA_CONNECTION_TIMEOUT=abc`, an unrecognized
`KAFKA_LOG_LEVEL`, a `KAFKA_SASL_MECHANISM` that cannot be built from environment variables alone
(`aws`, `oauthbearer`, `gssapi`, ...), a username set with no mechanism, or a mechanism set with no
username/password — is reported through `onDiagnostic` (a warning on stderr by default) and simply
left out of the result. It is never `NaN`, and one bad variable never stops the others from
resolving. Pass a custom `prefix` (default `'KAFKA_'`) to read a differently-prefixed set, e.g. for
multiple clusters in one process.

### Confluent/Bitnami collision

Confluent's and Bitnami's Kafka broker Docker images export their own `KAFKA_*` variables for the
_broker_ process. If your application shares a shell with one of those images, check which
`KAFKA_*` variables are actually present before calling `fromEnv` — a broker's `KAFKA_ADVERTISED_LISTENERS`
won't collide with anything on the allow-list above, but it is a sign the same prefix is doing two
jobs in that environment.

## `loadEnvFiles`

A `.env` file itself is loaded by `@cookiemonsterdev/kafka-config` — generic machinery with nothing
Kafka-specific about it, so it is not re-exported from `@cookiemonsterdev/kafka-core`:

```ts
import { loadEnvFiles } from '@cookiemonsterdev/kafka-config';

loadEnvFiles({ cwd: import.meta.dirname }); // loads ./.env, if present
```

It wraps Node's own `process.loadEnvFile()`: a variable already set in `process.env` is never
overridden, a missing file is reported in the result's `missing` array rather than thrown, and
multiple files load in order with the earliest file winning any variable more than one of them
sets. There is no `dotenv` dependency anywhere in this repository.

## `redactKafkaConfig`

Never log or print a `KafkaConfig`, a `KafkaFileConfig`, or `kafka.configSource()`'s output directly
— pass it through `redactKafkaConfig` first:

```ts
import { redactKafkaConfig } from '@cookiemonsterdev/kafka-core';

console.log(redactKafkaConfig(resolvedConfig));
```

It walks the whole value and replaces an explicit allow-list of secret-bearing fields with
`'[REDACTED]'`, wherever a `sasl` or `ssl` object appears at any depth:

- `sasl.password`, `sasl.secretAccessKey`, `sasl.sessionToken`, `sasl.tokenHmac`
- `ssl.key`, `ssl.pfx`, `ssl.passphrase`

Everything else survives the pass through unchanged. The result is always safe to
`JSON.stringify`: a function becomes `'[Function]'`, a `Buffer` becomes `'[Buffer N bytes]'`, and a
circular reference becomes `'[Circular]'` instead of throwing.

## See also

- [Config file reference](../config-file/) for `defineConfig`, discovery, and precedence.
- [Configuration guide](../../guides/configuration/) for a walkthrough that combines a config file
  with environment variables.
