---
title: Getting started
description: Install, build and run the workspace
order: 2
---

## Install

```sh
pnpm install
```

## Run the docs site

```sh
pnpm --filter @kafka/docs dev
```

## Build everything

```sh
pnpm build
```

`pnpm -r` walks the workspace in topological order, so `@kafka/core` is compiled
before the docs site that imports it.
