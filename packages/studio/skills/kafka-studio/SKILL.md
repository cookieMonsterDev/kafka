---
name: kafka-studio
description: >
  Use when someone wants a local web UI for an Apache Kafka cluster with
  @cookiemonsterdev/kafka-studio: installing and launching it (standalone kafka-studio or
  `kafka studio` from the CLI), pointing it at a broker or a kafka.config profile, running a
  throwaway local broker with Docker to try it, or exposing it safely with --read-only, --host,
  and --port.
metadata:
  purpose: >-
    Gets the studio running against the intended cluster with safe defaults, so a developer can
    browse topics, tail and produce messages, and inspect groups without writing code.
  type: lifecycle
  library: '@cookiemonsterdev/kafka-studio'
  library_version: '1.2.1'
sources:
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/studio/start/introduction.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/studio/start/installation.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/studio/guides/local-development.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/cli/reference/commands/studio.md'
---

# kafka-studio

A local web UI for a Kafka cluster: browse, create, and configure topics; produce messages
(single sends and rate-limited bursts); browse and tail live messages; inspect consumer groups
(members, lag, offset reset, deletion) and share groups; a live topology board; read-only views of
ACLs, quotas, and transactions. Served on `localhost`, built on `@cookiemonsterdev/kafka-core`.

## Setup

Requires Node.js 24+ or Bun 1.4+. Either path starts the same server.

```sh
# Standalone
npm install -g @cookiemonsterdev/kafka-studio
KAFKA_BROKERS=localhost:9092 kafka-studio

# Through the CLI (the CLI resolves the studio package at runtime)
npm install -g @cookiemonsterdev/kafka-cli @cookiemonsterdev/kafka-studio
kafka studio
```

It prints a `localhost` URL with a per-session token in the hash and opens a browser.

## Core Patterns

### Choose the cluster

- Plain local broker: `KAFKA_BROKERS=localhost:9092 kafka-studio`.
- SASL/SSL or several clusters: point `KAFKA_CONFIG` at a `kafka.config.*` file with
  `cli.profiles` (the same file the `kafka-cli` skill describes) and switch profiles from the
  sidebar.

### Try it without a cluster

The monorepo's `packages/studio/docker-compose.dev.yml` starts a single-node KRaft broker on
`localhost:9092` (PLAINTEXT). It is not shipped in the npm package; any local single-node Kafka
container works the same way.

```sh
docker compose -f docker-compose.dev.yml up -d
docker compose -f docker-compose.dev.yml ps        # wait for healthy
KAFKA_BROKERS=localhost:9092 kafka-studio
docker compose -f docker-compose.dev.yml down -v   # tear down and drop data
```

### Launch flags

```sh
kafka-studio --read-only                  # server rejects mutating API requests with 403
kafka-studio --port 5757 --browser none   # fixed port, no browser (default port: first free in 5757-5807)
kafka-studio --host 127.0.0.1             # default bind address
```

The same flags work on `kafka studio`. On Bun, use `bunx --bun kafka-studio`; the binary's shebang
otherwise runs it on Node.js.

## Common Mistakes

- **CRITICAL — Pointing a writable studio at production.** It can delete groups, reset offsets,
  and produce messages. Use `--read-only` for any shared or production cluster; it is enforced by
  the server, not just hidden in the UI.
- **HIGH — Binding to `0.0.0.0` or sharing the URL.** The session token in the URL grants access.
  Keep the default `127.0.0.1` bind and use SSH port forwarding for remote access.
- **MEDIUM — `kafka studio` prints an install hint.** The CLI does not depend on the studio; install
  `@cookiemonsterdev/kafka-studio` next to it.
- **MEDIUM — Looking for `docker-compose.dev.yml` in `node_modules`.** It exists only in the
  monorepo checkout.

## Completion

- The printed URL opens and the topic list shows the expected cluster's topics.
- For shared clusters, a create or delete action is refused (403) when `--read-only` is set.

## References

- Studio docs and HTTP API: https://cookiemonsterdev.github.io/kafka/docs/studio/start/introduction/
