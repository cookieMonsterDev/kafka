---
title: Installation
description: Install @cookiemonsterdev/kafka-core with npm, pnpm, yarn, or bun
order: 2
section: start
---

Node.js **24 or newer** is required (tested on 24 and 26); `zlib.zstd*` is used for ZSTD. It also
runs on Bun **1.4 or newer**; see [Bun](../../reference/bun/) for what differs there.

```sh
npm install @cookiemonsterdev/kafka-core
```

```sh
pnpm add @cookiemonsterdev/kafka-core
```

```sh
yarn add @cookiemonsterdev/kafka-core
```

```sh
bun add @cookiemonsterdev/kafka-core
```

TLS and SASL are optional. See [Security](../../guides/security/) when the broker
requires them. SASL/GSSAPI needs a KDC and either `sasl.gssProvider` or the
optional `kerberos` package (`npm install kerberos`).

Next: [Getting started](./getting-started/).
