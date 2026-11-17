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

## Native CRC32C

Record batches are checksummed with CRC32C on produce, and verified on consume
while `checkCrcs` is on (the default). The package manager installs the optional
[`@node-rs/crc32`](https://www.npmjs.com/package/@node-rs/crc32) package with
the client, and the client uses it to compute CRC32C with the CPU's CRC
instructions where available. It ships prebuilt binaries for Linux (glibc and
musl), macOS, Windows, FreeBSD, and Android. It has no install script, so Bun
needs no `trustedDependencies` entry for it.

The client loads the package the first time it checksums a record batch of 4 KiB
or more, and uses it for every batch after that. Until then, smaller batches use
the client's built-in JavaScript implementation, so a process that only produces
or consumes small batches doesn't spend the time and memory the package takes to
load. If the package isn't installed, or has no binary for the platform, the
client uses the JavaScript implementation for every batch. The JavaScript
implementation processes about 2.8 GB/s on an Apple silicon Mac on Node.js and
Bun. Both produce the same checksums.

To see which implementation is in use, call `getCrc32cImplementation()`. It
returns `'native'` or `'javascript'`, and loads the package if no record batch
of 4 KiB or more has loaded it yet:

```ts
import { getCrc32cImplementation } from '@cookiemonsterdev/kafka-core';

console.log(getCrc32cImplementation()); // 'native' when @node-rs/crc32 loaded
```

To leave the package out, install with `npm install --omit=optional`,
`pnpm install --no-optional`, or `bun install --omit=optional`.

Next: [Getting started](../getting-started/).
