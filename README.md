# kafka

A pnpm workspace containing:

| Package       | Path            | What it is                                      |
| ------------- | --------------- | ----------------------------------------------- |
| `@kafka/core` | `packages/core` | TypeScript Kafka client (Kafka 0.10+)           |
| `@kafka/docs` | `packages/docs` | Astro documentation site (Tailwind + shadcn/ui) |

## Requirements

- **Node.js 24** (Krypton LTS). The version is pinned in `.nvmrc`, and `engines`
  is enforced at install time — a wrong version fails the install rather than warning.
- **pnpm 11**, pinned via the `packageManager` field.

```sh
nvm use          # reads .nvmrc
corepack enable  # installs the exact pnpm version from package.json
```

## Local development

```sh
pnpm install     # installs every package, links @kafka/core into @kafka/docs
pnpm dev         # runs all packages' dev scripts in parallel
```

`pnpm dev` starts the docs site on <http://localhost:4321> and `tsc --watch` for
the library, so edits to `packages/core` are recompiled and picked up by the site.

## Workspace-wide commands

Run from the repo root. Each fans out to every package with `pnpm -r --if-present`,
so a package without that script is skipped instead of failing.

```sh
pnpm build       # build all packages, in dependency order
pnpm dev         # all dev servers/watchers, in parallel
pnpm typecheck   # tsc --noEmit + astro check
pnpm clean       # remove build output AND node_modules (re-run pnpm install after)
```

`pnpm -r` resolves the dependency graph, so `@kafka/core` always compiles before
the docs site that imports it.

## Working on one package

Use `--filter` from the root — no need to `cd`:

```sh
pnpm --filter @kafka/core build
pnpm --filter @kafka/docs dev
```

Add a dependency to a specific package:

```sh
pnpm --filter @kafka/docs add <pkg>
pnpm --filter @kafka/core add -D <pkg>
```

Include a package's own dependencies with the `...` suffix:

```sh
pnpm --filter @kafka/docs... build   # builds @kafka/core first, then the docs
```

Each package's README covers its own workflow: [`@kafka/core`](packages/core/README.md),
[`@kafka/docs`](packages/docs/README.md).

## Tests

```sh
pnpm --filter @kafka/core test
KAFKA_VERSION=0.10 pnpm --filter @kafka/core test:integration
KAFKA_VERSION=4.0 pnpm --filter @kafka/core test:integration
```

`KAFKA_VERSION` picks the Docker Compose stack. Unit tests never start Docker.
See [`packages/core/test/assets/README.md`](packages/core/test/assets/README.md).

## Shared dependency versions

TypeScript and Astro versions live in a single **catalog** in `pnpm-workspace.yaml`,
and packages reference them as `"typescript": "catalog:"`. Bump the version once
there and every package follows — they cannot drift apart.

```yaml
catalog:
  typescript: ^6.0.3
  astro: ^7.2.2
```

## Adding a package

1. Create `packages/<name>/package.json` with the name `@kafka/<name>`.
2. Run `pnpm install`.

The `packages/*` glob in `pnpm-workspace.yaml` picks it up — no config change needed.

To depend on another workspace package, use the `workspace:` protocol so it always
resolves locally rather than from the registry:

```sh
pnpm --filter @kafka/<name> add @kafka/core --workspace
```

## Configuration notes

pnpm 11 reads **only** auth and registry settings from `.npmrc`. Everything else
(`engineStrict`, `linkWorkspacePackages`, `catalog`, `allowBuilds`, …) must live in
`pnpm-workspace.yaml` — settings placed in `.npmrc` are silently ignored.

## License

[MIT](LICENSE) © Mykhailo Toporkov
