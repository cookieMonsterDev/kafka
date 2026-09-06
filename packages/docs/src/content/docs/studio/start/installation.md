---
title: Installation
description: Install @cookiemonsterdev/kafka-studio, standalone or through the CLI
order: 2
section: start
---

Node.js **24** is required.

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

## From this monorepo

Building from source instead of installing a release:

```sh
pnpm --filter @cookiemonsterdev/kafka-studio build
node packages/studio/dist/bin.js
```

Next: [Running it against a broker](../../guides/local-development/).
