# Agent operating contract

Instructions for AI coding agents working in this repository. This file is the
single source of truth for Cursor, Claude Code, Copilot, and similar tools.

Humans: see [CONTRIBUTING.md](CONTRIBUTING.md). Agents must follow that file
as well as this one. When process, style, tests, docs, commits, or PRs
conflict, [CONTRIBUTING.md](CONTRIBUTING.md) wins; this file adds
agent-specific constraints on top.

## What this is

`@cookiemonsterdev/kafka-core` is a TypeScript Apache Kafka client for Node.js that speaks the Kafka wire protocol directly (no native/JVM dependency): it negotiates API versions with the broker via `ApiVersions`, uses `bigint` for offsets, and supports brokers from Kafka 0.10 onward. `@cookiemonsterdev/kafka-docs` is the Astro documentation site. This is a pnpm workspace (`packages/core`, `packages/docs`); the workspace itself is not published to npm.

## Follow CONTRIBUTING.md

Read [CONTRIBUTING.md](CONTRIBUTING.md) before changing code. Do not invent a
parallel workflow. In particular:

- Prerequisites, local commands, shared catalog versions, and pnpm 11 notes
- Branch names: `<type>/<short-kebab-description>` from `develop`
- Conventional Commits (commitlint on `commit-msg` and in CI)
- One concern per branch and per PR; no mixed refactors or formatting-only noise
- Public API only from `packages/core/src/index.ts`; generated types, never a hand-written `index.d.ts`
- Tests and docs expectations under **Pull requests**, **Review expectations**, **Tests**, and **Documentation site** (including **Accessibility** for docs UI)
- Do not commit `dist/`, `.env`, certificates that are not already in the test fixtures, or secrets

The PR template (`.github/pull_request_template.md`) is the human-facing
version of the change checklist below. Fill it honestly when opening a PR.

## Change checklist

After every completed change (or batch), walk this list. Mark each item **done**
or **N/A** with a one-line reason. Do not skip silently.

1. **Scope** — One concern. No drive-by refactors, unrelated files, or hand-formatting (Prettier/ESLint run on commit and in CI).
2. **CONTRIBUTING.md** — Branch naming, commits, public API, tests, and docs match that file.
3. **Tests**
   - Protocol or public API change: unit tests beside source (`src/**/*.test.ts`).
   - New or changed broker behavior: integration tests in `packages/core/test/suites/**`, version-gated with helpers in `packages/core/test/helpers` (`testIfKafkaAtLeast_4_0`, `describeIfKRaft`, …). Do not parse `KAFKA_VERSION` in the test file.
   - Unit tests never start Docker. Integration tests need Docker unless `KAFKA_EXTERNAL=1`.
4. **Docs**
   - Public API, defaults, or compatibility: pages under `packages/docs/src/content/docs/core/` (sections: **start**, **guides**, **reference**, **migration**). How to add a page: `packages/docs/README.md`.
   - Workflow, commands, or package usage: the relevant package README and/or [CONTRIBUTING.md](CONTRIBUTING.md).
   - After `pnpm clean`, build core first (`pnpm --filter @cookiemonsterdev/kafka-docs... build`) because docs import `@cookiemonsterdev/kafka-core` from `dist/`.
   - UI change in `packages/docs` (layout, component, or CSS): follow **Documentation site → Accessibility** in [CONTRIBUTING.md](CONTRIBUTING.md). Keyboard, names, focus, contrast, and `prefers-reduced-motion` are in scope for the PR, not a follow-up.
5. **Exports** — New public surface is re-exported from `packages/core/src/index.ts` only. Types come from `tsc --emitDeclarationOnly`.
6. **Verification** — Run what the change needs from the repo root:
   - `pnpm lint`, `pnpm format:check`, `pnpm typecheck`
   - `pnpm test` (unit only)
   - Integration (`KAFKA_VERSION=… pnpm --filter @cookiemonsterdev/kafka-core test:integration`) if the change touches brokers, protocol, SASL, or admin
7. **Hygiene** — Filenames and folders are kebab-case. Nothing listed in CONTRIBUTING.md **Review expectations** is staged.
8. **Commit** — Suggest a Conventional Commit message. Do not commit unless asked.

## General rules

- After a completed task or batch of tasks, suggest a Conventional Commit message. Do not commit unless asked.
- Filenames and folders: kebab-case only (`fetch-request`, not `fetchRequest`).
- One concern per change. Do not mix a feature with a repo-wide reformat.
- Do not hand-format. Prettier and ESLint run on commit and in CI.

## Commands

Run from the repo root unless noted. `pnpm -r` walks the workspace dependency graph, so `@cookiemonsterdev/kafka-core` builds before `@cookiemonsterdev/kafka-docs` imports it.

```sh
nvm use && corepack enable && pnpm install   # Node 24 + pnpm 11 pinned, engineStrict enforced

pnpm build         # all packages, dependency order
pnpm lint          # ESLint (root)
pnpm format:check  # Prettier
pnpm typecheck     # tsc --noEmit (per package) + astro check
pnpm test          # unit tests only, never starts Docker
```

Single package / single test:

```sh
pnpm --filter @cookiemonsterdev/kafka-core test                       # unit tests (vitest --project unit)
pnpm --filter @cookiemonsterdev/kafka-core test -- fetch-request       # filter by filename/describe substring
pnpm --filter @cookiemonsterdev/kafka-core test:watch                  # watch mode
pnpm --filter @cookiemonsterdev/kafka-core typecheck                   # src + test/tsconfig.json

KAFKA_VERSION=0.10 pnpm --filter @cookiemonsterdev/kafka-core test:integration   # requires Docker
KAFKA_VERSION=4.3  pnpm --filter @cookiemonsterdev/kafka-core test:integration
# KAFKA_EXTERNAL=1 skips compose up/down; DO_NOT_STOP=1 leaves the cluster running
```

Unit tests live beside source as `*.test.ts` (`src/**/*.test.ts`) and never touch Docker. Integration tests live in `packages/core/test/suites/**` and select a `docker-compose*.yml` from `test/assets/` based on `KAFKA_VERSION` (default `4.0`) — see `packages/core/test/assets/README.md` for the version matrix and feature gates. Version-gate integration tests with the helpers in `packages/core/test/helpers` (`testIfKafkaAtLeast_4_0`, `describeIfKRaft`, etc.) instead of parsing `KAFKA_VERSION` directly.

Pre-commit runs ESLint + Prettier on staged files then `pnpm test`; commit-msg runs commitlint. Skip hooks for one command with `HUSKY=0` (avoid unless necessary).

Shared dependency versions (TypeScript, Vite, Vitest, Astro) live in the `catalog:` in `pnpm-workspace.yaml`; reference them as `"typescript": "catalog:"` rather than pinning per-package. `pnpm-workspace.yaml`, not `.npmrc`, is the source of truth for pnpm settings (`engineStrict`, `catalog`, `allowBuilds`, etc.) — pnpm 11 only reads auth/registry config from `.npmrc`. Depend on another workspace package with the `workspace:` protocol.

## Architecture

Everything public is re-exported from `packages/core/src/index.ts`; `src/client.ts` defines the `Kafka` class, the single entry point. One `Kafka` instance holds shared logging config and a committed-offsets map; each `.producer()`, `.consumer()`, `.admin()` call creates its own `Cluster` (connection pool).

Layered roughly bottom-up:

- **`protocol/`** — wire format. `protocol/requests/<api-name>/v<N>/{request,response}.ts` encode/decode one Kafka API at one version; `protocol/message`, `message-set`, `records` handle the record batch formats (legacy MessageSet v0/v1 and modern RecordBatch v2); `protocol/compression` holds codecs (GZIP/Snappy/LZ4/ZSTD built-in, overridable via `CompressionCodecs`); `protocol/sasl` handles SASL mechanism framing.
- **`network/`** — `socket.ts`/`socket-factory.ts` wrap `net`/`tls` sockets; `connection.ts` frames one broker connection (correlation IDs, request/response matching); `connection-pool.ts` and `request-queue/` manage concurrent in-flight requests per broker.
- **`broker/`** — one `Broker` per connection: version negotiation (`capabilities.ts` picks the highest mutually-supported API version), SASL handshake (`sasl-authenticator/`), and typed request/response methods built on `network/`.
- **`cluster/`** — `Cluster` (`cluster/index.ts`) owns broker discovery/metadata and a `BrokerPool` (`broker-pool.ts`) of live `Broker` connections, keyed by node id; `connection-pool-builder.ts` and `parse-broker-address.ts` turn the `brokers` config into pooled connections.
- **`producer/`**, **`consumer/`**, **`admin/`** — public-facing APIs built on `Cluster`. Producer: idempotence, transactions (`eos-manager/`), partitioners (`partitioners/`, default is murmur2, not the Java sticky partitioner — see compatibility notes). Consumer: group coordination, `assigners/` (range, round-robin default, sticky, cooperative-sticky), `offset-manager/` for commit/fetch tracking, classic group protocol by default, opt-in KIP-848 `groupProtocol: 'consumer'`.
- **`instrumentation/`** — event emitter for internal lifecycle events (connect, request, etc.).
- **`retry/`, `utils/`, `types/`, `loggers/`** — cross-cutting helpers; `errors.ts` at `src/` root defines all public `Kafka*Error` classes.

Key defaults that intentionally diverge from the Java client (full table in `packages/docs/src/content/docs/core/reference/compatibility.md`): `idempotent: false`, `read_committed` isolation by default, murmur2 partitioner. Produce constructor defaults are throughput-oriented (`lingerMs: 5`, `batchSize: 16384`, `maxInFlightRequests: 5`). Offsets are `bigint` everywhere, not strings.

## Code conventions

TypeScript is strict (`noUncheckedIndexedAccess`, `noImplicitOverride`), `verbatimModuleSyntax` (use `import type`), `erasableSyntaxOnly` (no enums, no parameter properties — Node's type-stripping must run the source as-is), `bundler` resolution with extensionless relative imports. Shared compiler options live in `tsconfig.base.json`; override per-package, not in the base.

- ESLint's type-checked rules apply to `packages/core/{src,test}`; `packages/core/src/protocol` (excluding tests) additionally forbids non-null assertions. `packages/docs` is not ESLint-linted (Prettier still applies to its Markdown/JSON/YAML/CSS; `*.astro` is Prettier-ignored).
- Public API surface is exported only from `packages/core/src/index.ts`. Types are generated via `tsc --emitDeclarationOnly` — never hand-write `index.d.ts`.
- Protocol and public API changes need tests. New broker behavior should use the version helpers in `packages/core/test/helpers` instead of parsing `KAFKA_VERSION` in the test file.
- Do not commit `dist/`, `.env`, certificates that are not already in the test fixtures, or secrets.

## Documentation

Markdown under `packages/docs/src/content/docs/<package>/<section>/` becomes a page (`/docs/core/start/introduction/`, and so on). Sections: **start**, **guides**, **reference**, **migration**. Nested folders become URL segments. After `pnpm clean`, build core first (`pnpm --filter @cookiemonsterdev/kafka-docs... build`) because docs import `@cookiemonsterdev/kafka-core` from `dist/`. Update docs or READMEs when the public API or workflow changes. How to add a page: `packages/docs/README.md`.

When the change touches docs **UI** (`packages/docs/src/{layouts,components,pages,styles}`), accessibility is part of the change, not optional polish. Match [CONTRIBUTING.md](CONTRIBUTING.md) **Documentation site → Accessibility**. In particular:

- Semantic HTML and landmarks first (`header`, `nav`, `main`, `article`); keep the skip link pointing at `#main-content`
- Icon-only controls need `aria-label`; decorative images/icons use `alt=""` / `aria-hidden="true"`
- Every control is keyboard-reachable with a visible `:focus-visible` ring; do not use `outline: none` without a replacement
- Honor `prefers-reduced-motion`; do not disable zoom (`user-scalable=no` / `maximum-scale=1`)
- Async status (copy, search, theme) uses `aria-live="polite"`; do not rely on color alone
- Text and UI contrast meet WCAG 2.2 AA; light `--muted-foreground` and `--ring` are sized for that

Markdown-only page edits still need a real `title` / `description` and should not introduce inaccessible patterns (images without `alt`, tables without headers).

## Commits and branches

Conventional Commits, enforced by commitlint on `commit-msg` and again in CI:

```
<type>(<optional scope>): <short imperative summary>
```

**Types:** `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `chore`, `ci`, `revert`

**Scope** (optional): `core`, `docs`, `protocol`, `consumer`, `producer`, `admin`, `network`, or another area of the change.

**Rules:** subject ≤72 characters, imperative mood (`add`, not `added`), no trailing period, one logical change per commit.

```
feat(core): add fetch request v4 encoder
fix(network): retry when the connection pool is exhausted
docs(getting-started): document lingerMs default
```

Branch from `develop`. Names: `<type>/<short-kebab-description>` (e.g. `fix/connection-pool-retry`). Full details, including the PR checklist and review expectations, are in [CONTRIBUTING.md](CONTRIBUTING.md).
