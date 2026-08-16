# @kafka/core

TypeScript library package. Compiles `src/` to `dist/` with type declarations.

## Local development

From the repo root (after `pnpm install`):

```sh
pnpm --filter @kafka/core dev        # tsc --watch, recompiles on save
pnpm --filter @kafka/core build      # one-off compile to dist/
pnpm --filter @kafka/core typecheck  # src + test/suites, tsc --noEmit
pnpm --filter @kafka/core clean      # remove dist/
```

Or from this directory, with the script name alone:

```sh
cd packages/core
pnpm dev
```

## Layout

```
src/index.ts   entry point — everything public is exported here
dist/          build output (git-ignored)
tsconfig.json  extends ../../tsconfig.base.json
```

Compiler options are shared: strict mode, `nodenext` modules, and
`erasableSyntaxOnly` (no enums or parameter properties, so the source stays
runnable by Node's native type stripping). Override per-package settings in
`tsconfig.json` rather than editing the base.

## Consuming this package

Consumers import from the package name, not a relative path:

```ts
import { Kafka, CompressionTypes, logLevel } from '@kafka/core';

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092'],
  logLevel: logLevel.INFO,
});
```

The `exports` field points at `dist/`, so **`dist/` must exist** before a
dependent package builds. `pnpm -r` handles this automatically by building in
dependency order — but if you build the docs site alone after a `clean`, build
this first:

```sh
pnpm --filter @kafka/docs... build   # the "..." includes dependencies
```

## Adding a dependency

```sh
pnpm --filter @kafka/core add <pkg>
pnpm --filter @kafka/core add -D <pkg>
```

For a version shared with other packages, add it to the `catalog:` in the root
`pnpm-workspace.yaml` and reference it as `"<pkg>": "catalog:"`.
