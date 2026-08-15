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

## Layout

```
astro.config.mjs               Astro config (Shiki themes, site URL)
src/content.config.ts          collection schema + glob loader
src/content/docs/*.md          the content
src/pages/index.astro          auto-generated index
src/pages/docs/[...slug].astro renders one page per Markdown file
src/layouts/BaseLayout.astro   shared HTML shell
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
