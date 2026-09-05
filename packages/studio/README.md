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

**Status:** early — the CLI, HTTP server, and web shell run end to end. The studio can browse,
create and configure topics; produce messages (single sends and rate-limited bursts); browse and
tail live messages; inspect consumer groups (members, per-partition lag, offset reset, deletion)
and share groups; and render a live topology board of cluster activity. Not published to npm;
install and usage instructions will be added once more of the plan lands.

## Contents

- [Local development](#local-development)
- [Local Kafka with Docker](#local-kafka-with-docker)
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
before starting the server from source — or run the one-liner below, which does both:

```sh
pnpm studio:dev
```

Builds `@cookiemonsterdev/kafka-config`, `@cookiemonsterdev/kafka-core`, and the studio itself, then
starts the server with `KAFKA_STUDIO_DEV=1` set, so `src/web` is served through Vite's dev
middleware (live reload) instead of the static `dist/web` build. Re-run it after a server-side
source change; `src/web` changes hot-reload on their own.

## Local Kafka with Docker

The studio has no built-in cluster — it needs a real (or locally hosted) broker to point at.
`docker-compose.dev.yml` in this package brings up a single-node KRaft broker on `localhost:9092`,
PLAINTEXT only, for exactly this:

```sh
cd packages/studio
docker compose -f docker-compose.dev.yml up -d
```

Wait for it to report healthy (`docker compose -f docker-compose.dev.yml ps`), then point the
studio at it with `KAFKA_BROKERS` — the studio (via `@cookiemonsterdev/kafka-core`'s `fromEnv`)
reads this the same way the CLI does, so no config file is required for a quick local check:

```sh
KAFKA_BROKERS=localhost:9092 node packages/studio/dist/bin.js
```

To use a named `cli.profiles` connection instead (SASL/SSL, a remote cluster, …), point
`KAFKA_CONFIG` at a `kafka.config.*` file the same way `@cookiemonsterdev/kafka-cli` does — see
[`@cookiemonsterdev/kafka-config`](../config/README.md).

Tear the broker down (and drop its data) with:

```sh
docker compose -f docker-compose.dev.yml down -v
```

This compose file is for manual, local use only — it is not part of `pnpm test` or
`pnpm test:integration` for this package. The broker-backed fixtures those eventually use live in
[`@cookiemonsterdev/kafka-core`](../core/README.md#tests)'s `test/assets/`.

## Design system

The UI is **dark-only** — there is no light palette, no `.dark` class, and no theme toggle. Every
token lives in the single `:root` block of `src/web/styles/theme.css`, and
`scripts/check-theme-drift.mjs` keeps that block byte-identical to
`packages/docs/src/styles/global.css`. Change one, change both, then run `pnpm theme:check` from
the workspace root.

The accent (`--primary`) is the brand mark's own green, and `--chart-1` … `--chart-5` are a
categorical set — `src/web/lib/topic-accent.ts` hashes a topic name onto one of them so a topic
keeps the same colour everywhere it appears.

Two colour literals are duplicated by hand because Vite does not process them through the module
graph: the `theme-color` meta and the splash screen in `src/web/index.html`. Both mirror
`--background`; keep them in sync when that token changes.

Loading and failure states go through the shared components in `src/web/components/ui/` —
`skeleton`, `spinner`, `empty-state`, `error-state`, `query-boundary` and `toast`. A failed read
gets a skeleton then an error panel with a working retry; a failed mutation always raises a toast
via the `QueryClient`'s `MutationCache`, so it survives the dialog that started it closing.

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
