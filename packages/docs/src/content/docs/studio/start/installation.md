---
title: Installation
description: Install @cookiemonsterdev/kafka-studio from npm, through the CLI, or as a standalone binary
order: 2
section: start
---

Node.js **24 or newer** (tested on 24 and 26) or Bun **1.4 or newer** is required.

## Alongside the CLI

Install both packages, then run the CLI's `studio` command:

```sh
npm install -g @cookiemonsterdev/kafka-cli @cookiemonsterdev/kafka-studio
kafka studio
```

`kafka studio` resolves `@cookiemonsterdev/kafka-studio` at runtime rather than depending on it
directly — the CLI stays installable on its own, and running `kafka studio` without the studio
package installed prints an install hint instead of failing silently.

## Standalone

The studio also works with no CLI at all, through its own binary:

```sh
npm install -g @cookiemonsterdev/kafka-studio
kafka-studio
```

Either path prints a `localhost` URL (with a per-session token in the hash) and opens it in a
browser. Run `kafka-studio --help` (or `kafka studio --help`) for the flag list — port, host,
browser, and read-only mode.

## Bun

`kafka-studio` starts with a `#!/usr/bin/env node` shebang, so Bun's tools run it on Node.js
unless told otherwise. Pass `--bun` to `bunx` to run it on Bun instead:

```sh
bunx --bun @cookiemonsterdev/kafka-studio
```

After `bun add -g @cookiemonsterdev/kafka-studio`, run `bunx --bun kafka-studio`. Running
`kafka-studio` directly still uses Node.js. `kafka studio` runs the studio in the CLI's own process,
so it uses whichever runtime the CLI was started with. See [Bun](../../../core/reference/bun/) for
what else differs on Bun.

## Standalone binary

Each [studio release](https://github.com/cookieMonsterDev/kafka/releases?q=studio-v&expanded=true)
also attaches a single-file `kafka-studio` executable that bundles Bun and the web UI, for machines
without Node.js or Bun. Download the asset for your platform (for example
`kafka-studio-linux-x64`) and `kafka-studio-SHA256SUMS`, then verify, rename, and run it:

```sh
sha256sum -c --ignore-missing kafka-studio-SHA256SUMS
mv kafka-studio-linux-x64 kafka-studio
chmod +x kafka-studio
./kafka-studio
```

Use this binary rather than `kafka studio` from the standalone `kafka` binary, which can't load the
studio. Read [Standalone binaries](../../../core/reference/bun/#standalone-binaries) for the
platforms and the other differences from the npm package, including the macOS quarantine step.

## From this monorepo

Building from source instead of installing a release:

```sh
pnpm --filter @cookiemonsterdev/kafka-studio... build
node packages/studio/dist/bin.js
```

Next: [Running it against a broker](../../guides/local-development/).
