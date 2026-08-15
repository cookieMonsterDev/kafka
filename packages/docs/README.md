# @kafka/docs

Astro documentation site. Every Markdown file under `src/content/docs/` becomes
a page automatically.

## Local development

From the repo root (after `pnpm install`):

```sh
pnpm --filter @kafka/docs dev        # dev server at http://localhost:4321
pnpm --filter @kafka/docs build      # static site into dist/
pnpm --filter @kafka/docs preview    # serve the built dist/ locally
pnpm --filter @kafka/docs typecheck  # astro check
pnpm --filter @kafka/docs clean      # remove dist/ and .astro/
```

Or from this directory, with the script name alone:

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

Create a `.md` file in `src/content/docs/` with frontmatter:

```markdown
---
title: My Page
description: Optional, used for the meta description and index listing
order: 3
---

Content goes here.
```

It is published at `/docs/<filename>/` and appears in the index, sorted by
`order`. No routing or config changes are needed.

`title` is required and `order` defaults to `999` — the schema in
`src/content.config.ts` validates this at build time, so a typo in frontmatter
fails the build with a clear message instead of rendering a broken page.

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

Design tokens live as CSS variables in `src/styles/global.css` (base color `neutral`,
`radix-nova` preset). Dark mode is driven by the `dark` class on `<html>`.
`@tailwindcss/typography` is enabled there too — rendered Markdown uses `prose`, since
Tailwind's preflight would otherwise reset it to unstyled HTML.

## Layout

```
astro.config.mjs               Astro config (React, Tailwind, Shiki, site URL)
components.json                shadcn/ui config (style, aliases, base color)
src/content.config.ts          collection schema + glob loader
src/content/docs/*.md          the content
src/pages/index.astro          auto-generated index, uses Card + buttonVariants
src/pages/docs/[...slug].astro renders one page per Markdown file
src/layouts/BaseLayout.astro   shared HTML shell, imports global.css
src/components/ui/*            shadcn components (yours to edit)
src/components/ThemeToggle.tsx interactive React island
src/lib/utils.ts               cn() class-merge helper
src/styles/global.css          Tailwind entry + design tokens
```

Markdown is discovered by the `glob()` loader in `src/content.config.ts`. To
change where content lives, edit the `base` path there — the routing follows.

## Notes

- `astro check` needs `@astrojs/check`, already a devDependency here. Without it
  the command prompts to install interactively and will hang in CI.
- Import `z` from `astro/zod`, not from `astro:content` — the latter is
  deprecated in Astro 7.
- `site` in `astro.config.mjs` is a placeholder (`https://example.com`). Set it to
  the real deployment URL before publishing, since it is used for canonical URLs
  and sitemaps.
