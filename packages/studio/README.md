# @cookiemonsterdev/kafka-studio

<p>
  <a href="https://github.com/cookieMonsterDev/kafka/actions/workflows/ci.yml"><img src="https://github.com/cookieMonsterDev/kafka/actions/workflows/ci.yml/badge.svg?branch=develop" alt="CI" /></a>
  <a href="https://www.npmjs.com/package/@cookiemonsterdev/kafka-studio"><img src="https://img.shields.io/npm/v/@cookiemonsterdev/kafka-studio.svg" alt="npm" /></a>
  <a href="../../LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT License" /></a>
</p>

A local web UI for inspecting and interacting with an Apache Kafka cluster — browse and manage
topics, tail and produce messages, inspect consumer groups, and watch a live view of traffic
moving through the cluster. Launched from the command line and served on `localhost`, in the
spirit of Prisma Studio. Built on top of
[`@cookiemonsterdev/kafka-core`](../core/README.md) and
[`@cookiemonsterdev/kafka-config`](../config/README.md), and lives in the
[kafka monorepo](https://github.com/cookieMonsterDev/kafka).

**Status:** scaffolding only — no functionality yet, and not published to npm. Install and usage
instructions will be added once there's something to run.

## Contents

- [Local development](#local-development)
- [Tests](#tests)
- [Contributing](#contributing)
- [License](#license)

## Local development

From the workspace root:

```sh
pnpm --filter @cookiemonsterdev/kafka-studio build
pnpm --filter @cookiemonsterdev/kafka-studio dev
```

`build` produces the server bundle (`dist/`) and the browser SPA (`dist/web/`). `dev` currently
watches only the server bundle.

## Tests

```sh
pnpm --filter @cookiemonsterdev/kafka-studio test
pnpm --filter @cookiemonsterdev/kafka-studio test:integration
```

Unit tests live beside source as `src/**/*.test.ts` and never start Docker. Integration tests will
live under `test/suites/**`, version-gated the same way as
[`@cookiemonsterdev/kafka-core`](../core/README.md#tests).

## Contributing

See [CONTRIBUTING.md](../../CONTRIBUTING.md) at the workspace root.

## License

[MIT](../../LICENSE)
