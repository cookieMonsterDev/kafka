# Contributing

Thanks for helping. This repo is a pnpm workspace: `@cookiemonsterdev/kafka-core` is the TypeScript Kafka client, `@cookiemonsterdev/kafka-docs` is the Astro documentation site.

Please search existing [issues](https://github.com/cookieMonsterDev/kafka/issues) and [pull requests](https://github.com/cookieMonsterDev/kafka/pulls) before opening a new one. For a large or breaking change, open an issue first and agree on the shape before you write a lot of code. Bug fixes can go straight to a PR.

Coding agents use [AGENTS.md](AGENTS.md) as their operating contract. That file requires them to follow this document (tests, docs, commits, PRs).

## Prerequisites

- **Node.js 24** (pinned in `.nvmrc`). `engines` is enforced at install time.
- **pnpm 11**, pinned via `packageManager` in the root `package.json`.
- **Docker** only if you run integration tests.

```sh
nvm use          # reads .nvmrc
corepack enable  # installs the exact pnpm version from package.json
pnpm install
```

`pnpm install` also installs [Husky](https://typicode.github.io/husky/) hooks. Skip them for one command with `HUSKY=0`.

## Local development

From the repo root:

```sh
pnpm dev           # docs site on http://localhost:4321 + Vite watch for @cookiemonsterdev/kafka-core
pnpm build         # all packages, in dependency order
pnpm lint          # ESLint
pnpm format        # Prettier write
pnpm format:check  # Prettier check (CI)
pnpm typecheck     # tsc --noEmit + astro check
pnpm test          # unit tests only (never starts Docker)
pnpm clean         # build output and node_modules (re-run pnpm install after)
```

`pnpm -r` walks the workspace graph, so `@cookiemonsterdev/kafka-core` compiles before `@cookiemonsterdev/kafka-docs` imports it.

### One package

```sh
pnpm --filter @cookiemonsterdev/kafka-core build
pnpm --filter @cookiemonsterdev/kafka-docs dev
pnpm --filter @cookiemonsterdev/kafka-docs add <pkg>
pnpm --filter @cookiemonsterdev/kafka-core add -D <pkg>
pnpm --filter @cookiemonsterdev/kafka-docs... build   # "..." includes workspace dependencies
```

Each package README has the rest of its workflow: [`@cookiemonsterdev/kafka-core`](packages/core/README.md), [`@cookiemonsterdev/kafka-docs`](packages/docs/README.md).

### Shared versions

TypeScript, Vite, Vitest, and Astro versions live in the **catalog** in `pnpm-workspace.yaml`. Packages reference them as `"typescript": "catalog:"`. Bump the version once there.

To depend on another workspace package, use the `workspace:` protocol:

```sh
pnpm --filter @cookiemonsterdev/kafka-<name> add @cookiemonsterdev/kafka-core --workspace
```

## Branch names

Branch from **`develop`**. Use kebab-case, with a Conventional Commit **type** as the prefix:

```
<type>/<short-kebab-description>
```

| Prefix      | Use for                                      |
| ----------- | -------------------------------------------- |
| `feat/`     | New user-facing behavior                     |
| `fix/`      | Bug fix                                      |
| `docs/`     | README, site, comments                       |
| `test/`     | Tests only                                   |
| `refactor/` | Internal change with no behavior change      |
| `perf/`     | Performance                                  |
| `style/`    | Formatting only (rare; let Prettier do this) |
| `chore/`    | Tooling, deps, repo hygiene                  |
| `ci/`       | GitHub Actions                               |
| `build/`    | Build graph, Vite, package exports           |

Examples:

```
feat/fetch-request-v12
fix/connection-pool-retry
docs/getting-started-producer-example
test/integration-kafka-43
chore/upgrade-typescript
```

Keep the description short. One concern per branch.

## Commits

[Conventional Commits](https://www.conventionalcommits.org/), enforced by commitlint on `commit-msg` and again in CI on every commit in the PR:

```
<type>(<optional scope>): <short imperative summary>
```

**Types:** `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `chore`, `ci`, `revert`

**Scope** (optional): `core`, `docs`, `protocol`, `consumer`, `producer`, `admin`, `network`, or another area of the change.

**Rules:**

- Subject at most 72 characters
- Imperative mood (`add`, not `added` or `adds`)
- No trailing period
- One logical change per commit
- No co-author during committing

```
feat(core): add fetch request v4 encoder
fix(network): retry when the connection pool is exhausted
docs(getting-started): document lingerMs default
```

### Git hooks

| Hook         | What it runs                                                        |
| ------------ | ------------------------------------------------------------------- |
| `pre-commit` | ESLint + Prettier on staged files (`lint-staged`), then `pnpm test` |
| `commit-msg` | commitlint                                                          |

Integration tests are not in the hook. They need Docker. CI still runs them.

## Pull requests

1. Create a branch from up-to-date **`develop`** using the naming scheme above.
2. Implement the change. Add or update tests. Update docs or READMEs when the public API or workflow changes.
3. Push and open a PR against **`develop`**. The PR template is filled in for you.
4. Link the issue with `Closes #123` when there is one.
5. Wait for CI. The [CI workflow](.github/workflows/ci.yml) runs format, commitlint, typecheck, unit tests, and an integration matrix (Kafka 0.10, 2.4, 3.6, 4.0, 4.3 on PRs; the full matrix on `develop` and `master`).
6. [CODEOWNERS](.github/CODEOWNERS) requests a review from the maintainer.

A PR should do **one** thing. Do not mix a feature with a repo-wide reformat.

Releases are not done from topic PRs. Merge `develop` into `master` (merge commit, not squash) when you want to publish. See [Releasing](#releasing).

### Review expectations

- Protocol and public API changes need tests.
- New broker behavior should use the version helpers in `packages/core/test/helpers` (`testIfKafkaAtLeast_4_0`, `describeIfKRaft`, …) instead of parsing `KAFKA_VERSION` in the test file.
- Docs UI changes must keep [accessibility](#accessibility) (keyboard, names, focus, contrast, reduced motion).
- Do not commit `dist/`, `.env`, certificates that are not already in the test fixtures, or secrets.

## Code style

Do not hand-format. Prettier and ESLint run on commit and in CI.

**Prettier** (`.prettierrc.json`): print width 120, single quotes, trailing commas.

**TypeScript** (`tsconfig.base.json`):

- `strict`, `noUncheckedIndexedAccess`, `noImplicitOverride`
- `verbatimModuleSyntax` — use `import type` / inline type imports
- `erasableSyntaxOnly` — no TypeScript enums or parameter properties (Node type-stripping must still run the source)
- `bundler` module resolution; relative imports omit file extensions

**ESLint** (`eslint.config.js`): type-checked rules on `packages/core/{src,test}`. Protocol code (`packages/core/src/protocol`, excluding tests) forbids non-null assertions. `packages/docs` is not linted with ESLint; Prettier still applies to Markdown, JSON, YAML, and CSS. `*.astro` is Prettier-ignored.

**Filenames and folders:** kebab-case only (`fetch-request`, not `fetchRequest`).

**Public API:** export from `packages/core/src/index.ts`. Types are generated with `tsc --emitDeclarationOnly`; do not add a hand-written `index.d.ts`.

## Tests

Unit tests are protocol fixtures and never start Docker:

```sh
pnpm test
pnpm --filter @cookiemonsterdev/kafka-core test
```

Integration tests pick a Compose file from `KAFKA_VERSION` (default `4.0`):

```sh
KAFKA_VERSION=0.10 pnpm --filter @cookiemonsterdev/kafka-core test:integration
KAFKA_VERSION=4.0 pnpm --filter @cookiemonsterdev/kafka-core test:integration
KAFKA_VERSION=4.3 pnpm --filter @cookiemonsterdev/kafka-core test:integration
```

`KAFKA_EXTERNAL=1` skips compose up/down. `DO_NOT_STOP=1` leaves the cluster running. Mapping, feature gates, and the CI matrix: [`packages/core/test/assets/README.md`](packages/core/test/assets/README.md).

## Documentation site

Markdown under `packages/docs/src/content/docs/<package>/<section>/` becomes a page
(`/docs/core/start/introduction/`, and so on). After `pnpm clean`, build core first:

```sh
pnpm --filter @cookiemonsterdev/kafka-docs... build
# or
pnpm --filter @cookiemonsterdev/kafka-docs dev
```

How to add a page, shadcn/ui notes, and layout: [`packages/docs/README.md`](packages/docs/README.md).

### Accessibility

The docs site should meet **WCAG 2.2 Level AA**. Treat that as in-scope whenever you change
`packages/docs` UI (layouts, components, pages, CSS) or Markdown that introduces images,
tables, or interactive examples. Do not land a visual change and “fix a11y later”.

Required for UI changes:

- **Structure** — Use semantic HTML (`header`, `nav` with an accessible name, `main#main-content`,
  `article`) before ARIA. Keep the skip link in `BaseLayout.astro`. Headings stay hierarchical
  (`h1` then `h2` / `h3`); do not skip levels. Heading anchors need enough `scroll-margin-top`
  that the sticky header does not cover them.
- **Names** — Icon-only buttons and links have `aria-label`. Decorative images use `alt=""`;
  decorative SVGs use `aria-hidden="true"`. Brand names and code samples that must not be
  auto-translated use `translate="no"`.
- **Keyboard** — Every control is reachable and operable without a pointer. Visible
  `:focus-visible` (ring or outline) is required; never `outline: none` without a replacement.
  The mobile docs menu keeps `aria-expanded`, closes on Escape, and does not leave focus
  trapped after it closes. Overflowing code blocks and tables must be focusable so they can
  be scrolled from the keyboard.
- **Motion and zoom** — Honor `prefers-reduced-motion`. Do not set `user-scalable=no` or
  `maximum-scale=1` on the viewport.
- **Status** — Copy, search results, and theme changes announce through `aria-live="polite"`.
  Do not use color as the only indicator (current page, warning callouts, copied state).
- **Contrast and targets** — Text meets 4.5:1 (AA); UI focus indicators meet 3:1. Prefer at
  least 24×24 CSS pixels for hit targets (44×44 where it does not break the layout). Light
  `--muted-foreground` and `--ring` in `global.css` are tuned for this; do not lighten them
  for aesthetics.

Markdown-only edits: every page keeps a meaningful `title` and `description`. Images need
`alt`. Tables need header cells. Do not convey meaning with color or emoji alone.

When opening a PR that touches docs UI, check the accessibility box on the PR template and
note how you verified it (keyboard pass, zoom, reduced-motion, or a screen reader).

## Releasing

`develop` is the default integration branch. `master` is the release branch.

| Package                        | What a release does                                                                                           |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------- |
| `@cookiemonsterdev/kafka-core` | npm publish (`@cookiemonsterdev/kafka-core`), GitHub release, tag `core-vX.Y.Z`, `packages/core/CHANGELOG.md` |
| `@cookiemonsterdev/kafka-docs` | GitHub Pages + GitHub release, tag `docs-vX.Y.Z`, `packages/docs/CHANGELOG.md` (not published to npm)         |

1. Merge the release PR **`develop` → `master`** with a **merge commit** (do not squash: semantic-release reads every Conventional Commit since the last tag).
2. The [Release](.github/workflows/release.yml) workflow runs on `master`. `dorny/paths-filter` skips packages that did not change. You can also run it from **Actions → Release** (`package`: `core` / `docs` / `all`, `dry_run`: true to print the next version without publishing).
3. A bot PR **`master` → `develop`** updates `package.json` and changelogs. Merge that with a merge commit too.
4. To delete a test release: **Actions → Unrelease** (type `DELETE`). You cannot republish the same npm version after unpublish.

## Adding a package

1. Create `packages/<name>/package.json` with the name `@cookiemonsterdev/kafka-<name>` (kebab-case folder).
2. Run `pnpm install`.

The `packages/*` glob in `pnpm-workspace.yaml` picks it up.

## Configuration notes

pnpm 11 reads **only** auth and registry settings from `.npmrc`. Everything else (`engineStrict`, `linkWorkspacePackages`, `catalog`, `allowBuilds`, …) must live in `pnpm-workspace.yaml`. Settings placed in `.npmrc` are silently ignored.

## License

By contributing, you agree that your contributions are licensed under the [MIT License](LICENSE).
