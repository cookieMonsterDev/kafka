---
title: Configuration
description: kafka.config files, profiles, and resolution order
order: 1
section: guides
---

`kafka init` scaffolds a `kafka.config.ts` (or `.mjs`, with `--js`) file in the current directory —
`.ts` when a `tsconfig.json` or a `typescript` dependency is detected, `.mjs` otherwise:

```sh
kafka init
kafka init --force   # overwrite an existing file
```

```ts
export default {
  client: {
    brokers: ['localhost:9092'],
  },
};
```

Every connecting command resolves its options from, highest precedence first: **the command's own
flags → environment variables → the active `--profile` → the config file's `client` section →
built-in defaults**. The config file is discovered by walking upward from the current directory
looking for `kafka.config.{ts,mts,cts,js,mjs,cjs,json}` (or `.config/kafka.*`), stopping at the
first `.git`, `pnpm-workspace.yaml`, or workspace `package.json` — override that with
`--config-file <path>` or `KAFKA_CONFIG`.

## Profiles

Named alternates — e.g. one entry per cluster — go under a `cli.profiles` section and are selected
with `--profile <name>` or `KAFKA_PROFILE`:

```ts
export default {
  cli: {
    profiles: {
      staging: { brokers: ['staging-1:9092', 'staging-2:9092'] },
      production: {
        brokers: ['prod-1:9092', 'prod-2:9092'],
        sasl: { mechanism: 'plain', username: '...', password: '...' },
      },
    },
  },
};
```

```sh
kafka --profile staging topic list
kafka profiles          # lists every configured profile, marking the active one
```

## Diagnosing what got resolved

`kafka doctor` reports the config file path (if any), the active profile, whether the connection
actually works, and — when `Kafka`'s own `configSource()` is available — where each value came
from:

```sh
kafka doctor
kafka doctor --profile staging
```

Reach for it first whenever a command connects to the wrong cluster, or fails to connect at all.

Next: [Output and scripting with JSON](./output-and-scripting/).
