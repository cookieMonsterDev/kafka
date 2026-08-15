---
title: Introduction
description: What this monorepo contains
order: 1
---

This is a pnpm workspace monorepo with two packages:

- `@kafka/core` — the library package, written in TypeScript.
- `@kafka/docs` — this Astro site.

Any `.md` file placed under `src/content/docs/` is picked up automatically and
published at `/docs/<filename>/`. No routing changes needed.
