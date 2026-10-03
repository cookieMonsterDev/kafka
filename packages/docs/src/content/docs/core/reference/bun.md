---
title: Bun
description: Supported runtimes, running the CLI and studio on Bun, and what behaves differently there
order: 11
section: reference
---

Every package in this workspace (`@cookiemonsterdev/kafka-core`, `-config`, `-cli`, and `-studio`)
runs on Node.js and on Bun. This page covers what differs on Bun.

## Supported versions

| Runtime | Supported | Tested on |
| ------- | --------- | --------- |
| Node.js | `>=24`    | 24 and 26 |
| Bun     | `>=1.4`   | 1.4       |

Both ranges are declared in each package's `engines` field. The CLI and studio check the runtime
when they start and exit with a clear message on an older Bun or Node.js. Under Bun,
`process.versions.node` reports a Node.js compatibility version, so the check reads
`process.versions.bun` instead. Do the same if your own code needs to detect Bun.

Install the packages with `bun add` as usual. The monorepo itself is developed with pnpm.

## Running the CLI and studio

The `kafka` and `kafka-studio` binaries start with a `#!/usr/bin/env node` shebang, so `bunx` and
`bun run` start them on Node.js unless you pass `--bun` before the package name:

```sh
bunx --bun @cookiemonsterdev/kafka-cli ping --brokers localhost:9092
bunx --bun @cookiemonsterdev/kafka-studio
```

The CLI and studio installation pages have the full details:
[CLI on Bun](../../../cli/start/installation/#bun) and
[studio on Bun](../../../studio/start/installation/#bun).

## Config loading

[Config file](../config-file/) loading works the same way on both runtimes, with these differences
on Bun:

- Bun transpiles TypeScript natively, so a `kafka.config.ts` with a TypeScript `enum` or an
  extensionless relative import loads without the Node.js transform fallback, and without its
  warning. On Node.js, write erasable TypeScript instead.
- The synchronous loader used by `new Kafka()` can `require()` a config file that uses top-level
  `await`, so it does not throw `KafkaConfigRequiresAsyncError`.
- The deprecated `installConfigTransformHooks()` does nothing, because Bun's `node:module` has no
  `registerHooks`.

Synchronous TS/JS config loading in `new Kafka()` is deprecated on both runtimes, and the
`config.sync-load-deprecated` warning still appears on Bun. Prefer `await Kafka.fromConfig()`, or
use `kafka.config.json`.

## TLS

Bun's TLS and `node:crypto` are built on BoringSSL rather than OpenSSL, so some
[`ssl` options](../../guides/security/) that work on Node.js are unavailable:

- DSA and RSA-PSS keys and certificates
- CCM and OCB cipher modes, and chacha20-poly1305 through `node:crypto`
- OCSP stapling
- TLS-PSK (`pskCallback`)

TLS session resumption also doesn't carry over from one process to another. Brokers with standard
RSA or ECDSA certificates and default cipher suites work on both runtimes.

## Native optional packages

The optional native packages (`kerberos` for SASL/GSSAPI, and `snappy` and `lz4` for faster
compression) load on Bun through N-API, with two caveats:

- `bun install` doesn't run dependency install scripts by default, so the addon is never built.
  List each package you use in your app's `trustedDependencies`:

  ```json
  {
    "trustedDependencies": ["kerberos", "snappy", "lz4"]
  }
  ```

- An addon built for Node.js 24 doesn't load on Bun 1.4, which reports `NODE_MODULE_VERSION` 147.
  If you switch runtimes on an existing install, reinstall or rebuild the addon under Bun.

Without the native `snappy` and `lz4` packages, the client uses its built-in pure-JS codecs. Those
need no setup on either runtime.

## Performance and workers

- **CRC32C.** On Node.js, record batch checksums use `crypto.hash('crc32c')` when OpenSSL provides
  it. BoringSSL doesn't, so on Bun every checksum uses the built-in JavaScript implementation. The
  result is identical; only large batches see a small CPU cost.
- **GZIP and ZSTD** use `node:zlib` on both runtimes.
- **Snappy and LZ4** run on a `worker_threads` pool on both runtimes. Bun ignores worker
  `resourceLimits`. The client's pool doesn't set any, but if you run your own codecs in workers,
  memory caps you set there aren't enforced on Bun.

## How we test

CI runs every package's unit test suite on Node.js 24 and 26, and again on Bun 1.4. On each of
those runtimes, a smoke test then loads the built packages the way an application would: it
imports each one, runs the `kafka` and `kafka-studio` executables, round-trips Snappy and LZ4
through the compression workers (and checks that the process still exits on its own), and loads a
TypeScript config file. Integration tests against real brokers run on Node.js only.

See also: [Compatibility](../compatibility/) for broker versions and the implemented API surface.
