---
title: Running it against a broker
description: Point the studio at a real cluster, or a one-command local Docker broker
order: 1
section: guides
---

The studio has no built-in cluster — it needs a real (or locally hosted) broker to point at. It
resolves a connection the same way
[`@cookiemonsterdev/kafka-cli`](../../../cli/guides/configuration/) does: environment variables,
then a `kafka.config.*` file's `cli.profiles` section, so a project only has to configure its
cluster once for both tools to share.

## Quickest: `KAFKA_BROKERS`

No config file required for a plain, unauthenticated connection:

```sh
KAFKA_BROKERS=localhost:9092 kafka-studio
```

## Named profiles

To reach a cluster that needs SASL/SSL, point `KAFKA_CONFIG` at a `kafka.config.*` file — see
[`@cookiemonsterdev/kafka-config`](../../../config/start/introduction/) for the file's own format —
and switch between its `cli.profiles` entries from the sidebar's profile switcher, or the
`/api/profiles/active` endpoint directly.

## A local broker with Docker

`@cookiemonsterdev/kafka-studio`'s package ships a `docker-compose.dev.yml` for exactly this: a
single-node KRaft broker on `localhost:9092`, PLAINTEXT only, for trying the studio against a real
cluster on a laptop.

```sh
cd packages/studio   # inside a checkout of the monorepo
docker compose -f docker-compose.dev.yml up -d
```

Wait for it to report healthy, then point the studio at it:

```sh
docker compose -f docker-compose.dev.yml ps
KAFKA_BROKERS=localhost:9092 kafka-studio
```

Tear it down (and drop its data) when done:

```sh
docker compose -f docker-compose.dev.yml down -v
```

This compose file is for manual, local use only — it isn't part of the package's own automated
tests. Its broker-backed integration suite runs against the same fixtures
[`@cookiemonsterdev/kafka-core`](../../../core/start/introduction/) uses for its own tests, brought
up automatically for that suite only.

Next: [API reference](../../reference/api/).
