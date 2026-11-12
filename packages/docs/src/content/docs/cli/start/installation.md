---
title: Installation
description: Run @cookiemonsterdev/kafka-cli with npx, pnpm dlx, a global install, or a standalone binary
order: 2
section: start
---

Node.js **24 or newer** (tested on 24 and 26) or Bun **1.4 or newer** is required. No install is
needed to try it once:

```sh
npx @cookiemonsterdev/kafka-cli ping --brokers localhost:9092
```

```sh
pnpm dlx @cookiemonsterdev/kafka-cli topic list --brokers localhost:9092
```

Or install it once, either globally or as a dev dependency of a project that talks to Kafka:

```sh
npm install -g @cookiemonsterdev/kafka-cli
kafka --version
```

```sh
npm install --save-dev @cookiemonsterdev/kafka-cli
```

## Bun

The `kafka` binary starts with a `#!/usr/bin/env node` shebang, so Bun's tools run it on Node.js
unless told otherwise. Pass `--bun` to `bunx` to run it on Bun instead:

```sh
bunx --bun @cookiemonsterdev/kafka-cli ping --brokers localhost:9092
```

After a global install with Bun, `bunx --bun kafka` runs the installed binary on Bun. Running
`kafka` directly still uses Node.js. Pass the binary's path to `bun` to skip the shebang:

```sh
bun add -g @cookiemonsterdev/kafka-cli
bunx --bun kafka --version
bun "$(command -v kafka)" --version
```

In a project that has the CLI as a dev dependency, `bunx --bun kafka` and `bun --bun kafka` both
run the local binary on Bun. See [Bun](../../../core/reference/bun/) for what else differs on Bun.

## Standalone binary

Each [CLI release](https://github.com/cookieMonsterDev/kafka/releases?q=cli-v&expanded=true) also
attaches a single-file `kafka` executable that bundles Bun, for machines without Node.js or Bun.
Download the asset for your platform (for example `kafka-linux-x64`) and `kafka-SHA256SUMS`, then
verify, rename, and run it:

```sh
sha256sum -c --ignore-missing kafka-SHA256SUMS
mv kafka-linux-x64 kafka
chmod +x kafka
./kafka --version
```

Read [Standalone binaries](../../../core/reference/bun/#standalone-binaries) for the platforms and
how the binary differs from the npm package, including the macOS quarantine step.

## Shell completion

Shell completion (bash, zsh, fish) is generated on demand — nothing to install separately:

```sh
eval "$(kafka completion bash)"   # or: zsh, fish
```

Next: [Getting started](../getting-started/).
