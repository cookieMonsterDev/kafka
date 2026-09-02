<p align="center">
  <img src="public/logo-icon.svg" width="72" height="72" alt="@cookiemonsterdev/kafka-docs">
</p>

# @cookiemonsterdev/kafka-docs

<p>
  <a href="https://github.com/cookieMonsterDev/kafka/actions/workflows/pages.yml"><img src="https://github.com/cookieMonsterDev/kafka/actions/workflows/pages.yml/badge.svg?branch=master" alt="Pages" /></a>
  <a href="../../LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT License" /></a>
</p>

The Astro-powered site behind [cookiemonsterdev.github.io/kafka](https://cookiemonsterdev.github.io/kafka/), documenting [`@cookiemonsterdev/kafka-core`](../core/README.md). Every Markdown file under `src/content/docs/` becomes a page — there's no separate step to wire up routing.

This package lives in the [kafka](https://github.com/cookieMonsterDev/kafka) workspace. It is not a published npm library.

## Contents

- [What is documented](#what-is-documented)
- [Local development](#local-development)
- [Adding a page](#adding-a-page)
- [Adding a package](#adding-a-package)
- [UI: shadcn/ui](#ui-shadcnui)
- [Layout](#layout)
- [Notes](#notes)
- [Releasing](#releasing)
- [Contributing](#contributing)
- [License](#license)

## What is documented

Markdown under `src/content/docs/<package>/<section>/` becomes a page. The
current package is **core**; later packages (cli, GUI) get their own folder
and `/docs/<package>/…` prefix. Sections:

- **Start** — introduction, installation, getting started
- **Guides** — producer, consumer, admin, errors, security, testing
- **Reference** — Kafka client, producer/consumer/admin APIs, configuration, error catalog, public API, compatibility
- **Migration** — breaking changes

Nested folders become URL segments (`/docs/core/start/introduction/`). Older
slugs (`/docs/start/introduction/`, `/docs/introduction/`,
`/docs/getting-started/`, `/docs/compatibility/`, `/docs/public-api/`,
`/docs/migration/`) redirect.

## Local development

From the repo root (after `pnpm install`):

```sh
pnpm --filter @cookiemonsterdev/kafka-docs dev        # http://localhost:4321
pnpm --filter @cookiemonsterdev/kafka-docs build      # static site into dist/
pnpm --filter @cookiemonsterdev/kafka-docs preview    # serve the built dist/ locally
pnpm --filter @cookiemonsterdev/kafka-docs typecheck  # astro check
pnpm --filter @cookiemonsterdev/kafka-docs clean      # remove dist/ and .astro/
```

Or from this directory:

```sh
cd packages/docs
pnpm dev
```

The dev server hot-reloads on Markdown edits. To use a different port:

```sh
pnpm --filter @cookiemonsterdev/kafka-docs dev --port 3000
```

> This package imports `@cookiemonsterdev/kafka-core`, which resolves to `packages/core/dist/`.
> After a clean checkout or `pnpm clean`, build it first — `pnpm --filter @cookiemonsterdev/kafka-docs... build`
> (the `...` suffix includes dependencies), or just `pnpm build` from the root.

## Adding a page

Create a `.md` file under `src/content/docs/core/<section>/` with frontmatter:

```markdown
---
title: My Page
description: Used for the meta description, sidebar, and index listing
order: 3
section: guides
---

Content goes here.
```

`section` must be one of `start`, `guides`, `reference`, `integrations`,
`migration`. Optional `sidebarLabel` overrides the title in the left nav.

The page is published at `/docs/core/<section>/<filename>/`, appears in the
sidebar under that section, and is sorted by `order` within the section.
No routing changes are needed.

Use **URL-relative** internal links, not file-relative `../` (one level) or
root-absolute `/docs/core/…` paths. Pages live at
`/docs/core/<section>/<filename>/` (plus the `/kafka` base on GitHub Pages).
From another section use `../../guides/testing/`; within the same section use
`./installation/`. Root-absolute `/docs/…` links omit the GitHub Pages base;
file-relative `../guides/…` resolves to `/docs/core/start/guides/…` and 404s.

`title`, `description`, and `section` are required. `order` defaults to
`999`. The schema in `src/content.config.ts` validates this at build time,
so a typo in frontmatter fails the build instead of rendering a broken page.

`Callout` and `CodeTabs` live in `src/components/` for use from `.astro`
(and later MDX). Prefer static HTML; do not hydrate them.

## Adding a package

Docs are grouped by package folder (`core` today; later `cli`, `gui`, …). Each
package gets its own sidebar. The package switcher at the top of the sidebar
lists whatever is in `DOCS_PACKAGES`.

To add a package:

1. Create `src/content/docs/<package>/<section>/*.md` as usual
2. Append the id to `DOCS_PACKAGES` in `src/lib/docs.ts`
3. Add a `label` and `blurb` in `DOCS_PACKAGE_META` (shown in the switcher)

The switcher links to that package’s first sidebar page. No layout changes.

## UI: shadcn/ui

Set up per the [Astro guide](https://ui.shadcn.com/docs/installation/astro), on top of
Tailwind v4 and the React integration. Components are copied into this repo — they are
source files you own and edit, not a dependency.

Add a component:

```sh
pnpm dlx shadcn@latest add <component>   # e.g. dialog, input, badge
```

It lands in `src/components/ui/` and is importable via the `@/*` alias.

<details>
<summary>Static vs. interactive components, the <code>asChild</code> gotcha, accessibility, and theming</summary>

### Two ways to use a component

**Static (no JavaScript shipped).** Rendering a React component from `.astro`
without a `client:*` directive produces plain HTML:

```astro
---
import { Card, CardHeader, CardTitle } from '@/components/ui/card'
---
<Card><CardHeader><CardTitle>Static</CardTitle></CardHeader></Card>
```

**Interactive (hydrated island).** Add a `client:*` directive. `src/components/ThemeToggle.tsx`
is the working example, mounted in `BaseLayout.astro`:

```astro
<ThemeToggle client:load />
```

Only that component's JavaScript is sent to the browser; the rest of the page stays static.

### Links: use `buttonVariants`, not `asChild`

**`asChild` does not work from `.astro` files.** Radix's `Slot` clones a React
child element, but Astro passes children as a slot rather than an element — so the
styling is silently dropped and you get an unstyled tag with no error.

```astro
---
import { buttonVariants } from '@/components/ui/button'
---
<!-- correct -->
<a href="/docs/core/" class={buttonVariants({ variant: 'secondary', size: 'sm' })}>Read</a>

<!-- silently renders a bare, unstyled <a> -->
<Button asChild><a href="/docs/core/">Read</a></Button>
```

`asChild` works normally _inside_ `.tsx` components, where children are real React elements.

### Accessibility

Target **WCAG 2.2 AA** for anything in `src/layouts`, `src/components`, `src/pages`, and
`src/styles`. Details and the PR expectation: [CONTRIBUTING.md](../../CONTRIBUTING.md#accessibility).

When you add or change UI:

- Prefer semantic HTML and named landmarks over extra ARIA
- Icon-only controls need `aria-label`; decorative icons `aria-hidden="true"`
- Keep a visible `:focus-visible` style; honor `prefers-reduced-motion`
- Announce copy / search / theme updates with `aria-live="polite"`
- Do not weaken `--muted-foreground` or `--ring` in `src/styles/global.css` below AA contrast
- Markdown images need `alt`; tables need header cells

### Theming

Design tokens live as CSS variables in `src/styles/global.css` (base color `olive`,
`radix-nova` preset `b6TpS6SrnE`). Dark mode is driven by the `dark` class on `<html>`.
`@tailwindcss/typography` is enabled there too — rendered Markdown uses `prose`, since
Tailwind's preflight would otherwise reset it to unstyled HTML.

</details>

## Layout

```
astro.config.mjs                 Astro config (React, Tailwind, Shiki, redirects, site URL)
components.json                  shadcn/ui config (style, aliases, base color)
public/                          favicon, apple-touch-icon, and logo assets
src/content.config.ts            collection schema + glob loader
src/content/docs/<package>/<section>/*.md  the content, grouped by package then section
src/pages/index.astro            landing hero (Get Started, Learn more, install)
src/pages/docs/[...slug].astro   one page per Markdown file (`/docs/core/…`)
src/layouts/BaseLayout.astro     HTML shell, header, docs search, GitHub link, theme toggle
src/components/docs-search.tsx   ⌘K documentation search dialog
src/layouts/docs-layout.astro    sidebar + article + on-this-page TOC
src/components/package-switcher.astro
src/components/package-switcher-select.tsx
src/components/docs-sidebar.astro
src/components/table-of-contents.astro
src/components/prev-next.astro
src/components/callout.astro
src/components/code-tabs.astro
src/components/ui/*              shadcn components (yours to edit)
src/components/copy-code.tsx     copy-icon buttons for the install chip and code blocks
src/components/ThemeToggle.tsx   interactive React island
src/lib/docs.ts                  sidebar grouping, prev/next, hrefs
src/lib/utils.ts                 cn() class-merge helper
src/styles/global.css            Tailwind entry + design tokens
```

Markdown is discovered by the `glob()` loader in `src/content.config.ts`. To
change where content lives, edit the `base` path there — the routing follows.

## Notes

- `astro check` needs `@astrojs/check`, already a devDependency here. Without it
  the command prompts to install interactively and will hang in CI.
- Import `z` from `astro/zod`, not from `astro:content` — the latter is
  deprecated in Astro 7.
- `site` in `astro.config.mjs` is `https://cookiemonsterdev.github.io`. GitHub Pages builds set `GITHUB_PAGES=1`, which prefixes URLs with `/kafka`. Local `pnpm dev` stays at `http://localhost:4321/`.

## Releasing

`@cookiemonsterdev/kafka-docs` is **private** and **not published to npm** — but it still gets a
proper release: a semver in `package.json`, a git tag, a GitHub release, and a changelog entry,
the same semantic-release flow as [`@cookiemonsterdev/kafka-core`](../core/CHANGELOG.md), minus the
npm publish step. Most contributors never need to trigger this by hand; a maintainer runs it from
`master`.

<details>
<summary>Full release mechanics: tags, versioning, dry-run, and rollback</summary>

| Artifact       | Docs                          | Core (for comparison)        |
| -------------- | ----------------------------- | ---------------------------- |
| Git tag        | `docs-vX.Y.Z`                 | `core-vX.Y.Z`                |
| Changelog      | `packages/docs/CHANGELOG.md`  | `packages/core/CHANGELOG.md` |
| GitHub release | yes                           | yes                          |
| npm publish    | no (`npmPublish: false`)      | yes                          |
| GitHub Pages   | yes (separate Pages workflow) | —                            |

Configuration lives in [`release.config.js`](./release.config.js). The
[Release](../../.github/workflows/release.yml) workflow runs on pushes to
`master` when `packages/docs/**` changed (or manually via **Actions → Release**,
`package`: `docs`).

### Commit types that bump the docs version

Only commits that touch files under `packages/docs/` count (semantic-release-monorepo).
Use Conventional Commits with a `docs` scope when the change is docs-only:

| Commit type                          | Docs version bump |
| ------------------------------------ | ----------------- |
| `feat`                               | minor             |
| `fix`                                | patch             |
| `docs`                               | patch             |
| `perf`                               | patch             |
| `refactor`                           | patch             |
| breaking (`!` or `BREAKING CHANGE:`) | major             |

Examples: `fix(docs): repair internal markdown links`, `feat(docs): add changelog page`.

### Dry-run (must be on `master` with full git history)

```sh
git checkout master && git pull
pnpm release:dry-run docs
# or from this package:
pnpm release:dry-run
```

Dry-run prints the next version and release notes without creating a tag, release, or commit.

### After a release

1. semantic-release commits `chore(docs): X.Y.Z [skip ci]` on `master` (updates
   `package.json` + `CHANGELOG.md`).
2. A bot opens **`master` → `develop`** to sync versions — merge it with a merge commit.
3. [GitHub Pages](../../.github/workflows/pages.yml) deploys the site (same push or the
   next one that touches `packages/docs/**`).
4. To remove a mistaken release: **Actions → Unrelease**, package `docs`, version `X.Y.Z`,
   confirm `DELETE` (drops tag + GitHub release; does not roll back Pages).

</details>

Full repo release process: [CONTRIBUTING.md § Releasing](../../CONTRIBUTING.md#releasing).

## Contributing

[CONTRIBUTING.md](../../CONTRIBUTING.md) — branch names, Conventional Commits, and PR flow.
Open a [documentation issue](https://github.com/cookieMonsterDev/kafka/issues/new?template=docs.yml) for typos or missing pages.

## License

[MIT](../../LICENSE) © Mykhailo Toporkov
