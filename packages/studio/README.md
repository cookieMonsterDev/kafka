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

**Status:** early — the CLI, HTTP server, and web shell run end to end, but there is no Kafka
connection yet (the cluster page just reports "not connected"). Not published to npm; install and
usage instructions will be added once there's a real feature to point at.

## Contents

- [Local development](#local-development)
- [Tests](#tests)
- [Contributing](#contributing)
- [License](#license)

## Local development

From the workspace root:

```sh
pnpm --filter @cookiemonsterdev/kafka-studio build
node packages/studio/dist/bin.js
```

`build` produces the server bundle (`dist/`) and the browser SPA (`dist/web/`). Run
`node packages/studio/dist/bin.js --help` for the flag list (port, host, browser, read-only mode).

`pnpm --filter @cookiemonsterdev/kafka-studio dev` watches and rebuilds the server bundle only. To
serve `src/web` with Vite's own dev server instead of a static build, set `KAFKA_STUDIO_DEV=1`
before starting the server from source.

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
