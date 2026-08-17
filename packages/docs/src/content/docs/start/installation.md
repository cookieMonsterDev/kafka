---
title: Installation
description: Install @kafka/core with npm, pnpm, yarn, or bun
order: 2
section: start
---

```sh
npm install @kafka/core
```

Click the block to copy. Node.js **24** is required (`zlib.zstd*` is used for ZSTD).

Until the package is published, install from this workspace instead:

```sh
pnpm install
pnpm --filter @kafka/core build
```

TLS and SASL are optional. See [Security](/docs/guides/security/) when the broker
requires them.

Next: [Getting started](/docs/start/getting-started/).
