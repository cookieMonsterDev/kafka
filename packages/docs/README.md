<p align="center">
  <img src="public/logo-icon.svg" width="72" height="72" alt="@kafka/docs">
</p>

# @kafka/docs

Astro documentation site for [`@kafka/core`](../core/README.md). Every Markdown file under `src/content/docs/` becomes a page.

This package lives in the [kafka](https://github.com/cookieMonsterDev/kafka) workspace. It is not a published npm library.

## What is documented

Markdown under `src/content/docs/<section>/` becomes a page. Sections:

- **Start** — introduction, installation, getting started
- **Guides** — producer, consumer, admin, errors, security, testing
- **Reference** — Kafka client, producer/consumer/admin APIs, configuration, error catalog, public API, compatibility
- **Migration** — breaking changes

Nested folders become URL segments (`/docs/start/introduction/`). The old
flat slugs (`/docs/introduction/`, `/docs/getting-started/`,
`/docs/compatibility/`, `/docs/public-api/`, `/docs/migration/`) redirect.

## Local development

From the repo root (after `pnpm install`):

```sh
pnpm --filter @kafka/docs dev        # http://localhost:4321
pnpm --filter @kafka/docs build      # static site into dist/
pnpm --filter @kafka/docs preview    # serve the built dist/ locally
pnpm --filter @kafka/docs typecheck  # astro check
pnpm --filter @kafka/docs clean      # remove dist/ and .astro/
```

Or from this directory:

```sh
cd packages/docs
pnpm dev
```

The dev server hot-reloads on Markdown edits. To use a different port:

```sh
pnpm --filter @kafka/docs dev --port 3000
```

> This package imports `@kafka/core`, which resolves to `packages/core/dist/`.
> After a clean checkout or `pnpm clean`, build it first — `pnpm --filter @kafka/docs... build`
> (the `...` suffix includes dependencies), or just `pnpm build` from the root.

## Adding a page

Create a `.md` file under `src/content/docs/<section>/` with frontmatter:

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

The page is published at `/docs/<section>/<filename>/`, appears in the
sidebar under that section, and is sorted by `order` within the section.
No routing changes are needed.

`title`, `description`, and `section` are required. `order` defaults to
`999`. The schema in `src/content.config.ts` validates this at build time,
so a typo in frontmatter fails the build instead of rendering a broken page.

`Callout` and `CodeTabs` live in `src/components/` for use from `.astro`
(and later MDX). Prefer static HTML; do not hydrate them.

## UI: shadcn/ui

Set up per the [Astro guide](https://ui.shadcn.com/docs/installation/astro), on top of
Tailwind v4 and the React integration. Components are copied into this repo — they are
source files you own and edit, not a dependency.

Add a component:

```sh
pnpm dlx shadcn@latest add <component>   # e.g. dialog, input, badge
```

It lands in `src/components/ui/` and is importable via the `@/*` alias.

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
<a href="/docs/" class={buttonVariants({ variant: 'secondary', size: 'sm' })}>Read</a>

<!-- silently renders a bare, unstyled <a> -->
<Button asChild><a href="/docs/">Read</a></Button>
```

`asChild` works normally _inside_ `.tsx` components, where children are real React elements.

### Theming

Design tokens live as CSS variables in `src/styles/global.css` (base color `olive`,
`radix-nova` preset `b6TpS6SrnE`). Dark mode is driven by the `dark` class on `<html>`.
`@tailwindcss/typography` is enabled there too — rendered Markdown uses `prose`, since
Tailwind's preflight would otherwise reset it to unstyled HTML.

## Layout

```
astro.config.mjs                 Astro config (React, Tailwind, Shiki, redirects, site URL)
components.json                  shadcn/ui config (style, aliases, base color)
public/                          favicon, apple-touch-icon, and logo assets
src/content.config.ts            collection schema + glob loader
src/content/docs/<section>/*.md  the content, grouped by sidebar section
src/pages/index.astro            landing hero (Get Started, Learn more, install)
src/pages/docs/[...slug].astro   one page per Markdown file
src/layouts/BaseLayout.astro     HTML shell, header, GitHub link, theme toggle
src/layouts/docs-layout.astro    sidebar + article + on-this-page TOC
src/components/docs-sidebar.astro
src/components/table-of-contents.astro
src/components/prev-next.astro
src/components/callout.astro
src/components/code-tabs.astro
src/components/ui/*              shadcn components (yours to edit)
src/components/copy-code.tsx     click-to-copy for install chip and code blocks
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

## Contributing

[CONTRIBUTING.md](../../CONTRIBUTING.md) — branch names, Conventional Commits, and PR flow.
Open a [documentation issue](https://github.com/cookieMonsterDev/kafka/issues/new?template=docs.yml) for typos or missing pages.

## License

[MIT](../../LICENSE) © Mykhailo Toporkov
