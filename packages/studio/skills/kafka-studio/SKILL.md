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
  library_version: '1.3.0'
sources:
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/studio/guides/debugger.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/studio/reference/api.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/studio/start/introduction.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/studio/start/installation.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/studio/guides/local-development.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/cli/reference/commands/studio.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/studio/start/getting-started.md'
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

Where neither runtime is installed, each studio GitHub release also attaches a standalone
`kafka-studio-<os>-<arch>` executable that bundles Bun and the web UI. Use it instead of
`kafka studio` from the standalone `kafka` binary, which cannot load the studio.

From the monorepo, build Studio and its workspace dependencies with
`pnpm --filter @cookiemonsterdev/kafka-studio... build`, then run `node packages/studio/dist/bin.js`.

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

### Browse and recover a session

- From a topic, use **Browse messages** or **Produce** to preselect it on the next page.
- Filter configuration by key or choose **Overrides only**; pending edits survive filter changes.
- **Live** selects the new-message stream; switch to **History** to stop it.
- **Studio uptime** describes the local process, not broker uptime.
- If the session recovery screen appears after a reload or restart, open the full URL printed
  in the launching terminal in a new tab. Retry cannot recover an in-memory token.

### Use the docked terminal

- Open **Terminal** in the top bar; drag the separator or use its arrow keys to resize. Expand,
  restore, minimize or end the session with its header controls.
- Run `help`, `topic list`, `topic create orders --partitions 3`, or `group offsets readers`.
  Commands use the embedded CLI and active Studio profile; `--help` and `--json` behave like CLI.
- `produce orders --value '{"id":1}'` sends a message. `consume orders --limit 10` reads stored
  records; `consume orders --tail` follows new ones until Ctrl+C.
- Tab completes, arrows edit/recall history, Ctrl+C interrupts, Ctrl+L or `clear` clears the display,
  Shift+Tab leaves focus, and `exit` ends the session. Destructive commands retain CLI confirmation.
- Navigation and minimizing retain the session. Changing profiles or ending it starts a new shell.
- Server-side read-only mode denies writes and produce. Host-file and connection flags, local
  CLI commands, OS commands and pipelines are unavailable.

### Debug event flows

- Date-time fields use a calendar popover with hour/minute selectors; **Now**, **Clear**, and **Done**
  keep values in local time.
- Open **Debugger** for bounded field/header searches and cross-topic correlation. Check scan
  coverage before treating no matches or compacted state as complete. Search predicates use
  JSON literals joined by `AND`; offsets/timestamps are quoted decimal strings.
- Stage selected records for replay; inspect destination, bytes, and rate. Replay preserves
  tombstones and repeated headers. Cancellation cannot undo writes; uncertain sends need inspection.
- Configure `studio.schemaRegistries.default` or a matching named-profile entry for Avro, JSON
  Schema, and Protobuf codecs. Protobuf JSON accepts exact decimal-string int64 values and enum names.
  Credentials stay server-side. No automatic schema registration.
- Export scenarios and use standalone `kafka-studio --scenario file.json --profile staging`.
  Exit 0 means pass, 1 means failure. These flags do not work in the embedded terminal or CLI wrapper.
- Use **Compare** to read config/schema differences without switching profiles. Group pages
  collect bounded session lag history; offset rates do not prove application processing.

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

## Documentation workflow

Follow the first-topic walkthrough to connect, produce valid JSON, and inspect message history. Configure brokers through KAFKA_BROKERS or a config profile; kafka-studio has no --brokers flag. Never include the session token in screenshots or shared URLs.
