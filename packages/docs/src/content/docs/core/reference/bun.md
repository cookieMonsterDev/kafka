---
title: Bun
description: Supported runtimes, Bun-native code paths, standalone binaries, and what behaves differently on Bun
order: 11
section: reference
---

Every package in this workspace (`@cookiemonsterdev/kafka-core`, `-config`, `-cli`, and `-studio`)
runs on Node.js and on Bun. This page covers what differs on Bun.

## Supported versions

| Runtime | Supported | Tested on                |
| ------- | --------- | ------------------------ |
| Node.js | `>=24`    | 24 and 26                |
| Bun     | `>=1.4`   | 1.4.x and latest release |

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
[studio on Bun](../../../studio/start/installation/#bun). Both are also released as
[standalone binaries](#standalone-binaries) that bundle Bun and need no runtime installed.

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
  List each package you use in your app's
  [`trustedDependencies`](https://bun.com/docs/pm/lifecycle#trusteddependencies):

  ```json
  {
    "trustedDependencies": ["kerberos", "snappy", "lz4"]
  }
  ```

- An addon built for Node.js 24 doesn't load on Bun 1.4, which reports `NODE_MODULE_VERSION` 147.
  If you switch runtimes on an existing install, reinstall or rebuild the addon under Bun.

Without the native `snappy` and `lz4` packages, the client uses its built-in pure-JS codecs. Those
need no setup on either runtime.

## Bun-native code paths

On Bun, the client calls a Bun API instead of Bun's Node.js compatibility layer only where a
benchmark showed a clear gain with the same behavior. The npm packages ship one build for both
runtimes and choose the code path when they run, so there is nothing to configure. For how the
client compares with other Kafka clients on Bun, see [Benchmarks](../benchmarks/).

| Area               | On Bun                 | Why                                                                                                                                                                                                                                                 |
| ------------------ | ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ZSTD compression   | `Bun.zstdCompress`     | Faster than `node:zlib` on Bun for 16 KiB to 1 MiB batches, and also asynchronous. Used when `compressionLevel` is unset or an integer from 1 to 22. Other levels, which it rejects, use `node:zlib`.                                               |
| ZSTD decompression | `node:zlib`            | Bun's decompression APIs take no output size limit, so the client couldn't cap how large a fetched batch may grow when decompressed.                                                                                                                |
| GZIP               | `node:zlib`            | Bun's GZIP APIs are synchronous. They block the event loop, and they're slower than `node:zlib` from 64 KiB or with several batches in flight.                                                                                                      |
| CRC32C             | JavaScript             | Bun has no CRC32C API (`Bun.hash.crc32` computes the IEEE CRC-32). See [CRC32C](#crc32c).                                                                                                                                                           |
| Sockets and TLS    | `node:net`, `node:tls` | `Bun.connect` stayed within 10% of Bun's `node:net`: faster for small requests, slower for 1 MiB produce requests. Its TLS options also can't express every [`ssl` option](../../guides/security/), such as `checkServerIdentity` and `minVersion`. |
| SCRAM              | `node:crypto`          | `pbkdf2` from `node:crypto` is already fast on Bun. An implementation on `Bun.CryptoHasher` was slower.                                                                                                                                             |
| Studio HTTP server | `node:http`            | The studio's development mode mounts Vite's middleware, which needs `node:http`. `Bun.serve` also closes connections idle for 10 seconds by default, so each of the studio's event streams would need its own override.                             |

A ZSTD batch compressed with `Bun.zstdCompress` is a standard frame that any broker or consumer
decodes, but its bytes can differ from what `node:zlib` produces for the same input.

To rule out a Bun-native path while debugging, set `KAFKA_BUN_NATIVE=0` in the environment. It
turns off every Bun-native path, which today means ZSTD compression uses `node:zlib` as on Node.js.
The variable is read on each use and has no effect on Node.js.

### CRC32C

Record batch checksums use `crypto.hash('crc32c')` when the platform's crypto library provides it.
Stock Node.js 24 and 26 builds don't, and neither does Bun, so both runtimes normally use the
built-in JavaScript implementation. It processes about 2.8 GB/s on an Apple silicon Mac on either
runtime.

### Compression workers

Snappy and LZ4 run on a `worker_threads` pool on both runtimes. Bun ignores worker
`resourceLimits`. The client's pool doesn't set any, but if you run your own codecs in workers,
memory caps you set there aren't enforced on Bun.

## Standalone binaries

Each CLI and studio release on GitHub attaches standalone `kafka` and `kafka-studio` executables,
compiled with [`bun build --compile`](https://bun.com/docs/compiler/executables) from the same code
as the npm packages. They embed Bun, so the machine needs neither Node.js nor Bun. Installation
steps: [CLI](../../../cli/start/installation/#standalone-binary) and
[studio](../../../studio/start/installation/#standalone-binary).

| Platform            | Asset name suffix |
| ------------------- | ----------------- |
| Linux x64           | `linux-x64`       |
| Linux arm64         | `linux-arm64`     |
| macOS Apple silicon | `darwin-arm64`    |
| Windows x64         | `windows-x64.exe` |

Each release also attaches `kafka-SHA256SUMS` or `kafka-studio-SHA256SUMS` with the checksums. The
binaries differ from the npm packages in these ways:

- There is no Intel macOS (`darwin-x64`) build. Install from npm there.
- The macOS build is ad-hoc signed, not notarized. macOS quarantines a file downloaded with a
  browser and refuses to open it until you remove the attribute:
  `xattr -d com.apple.quarantine kafka`.
- The [optional native packages](#native-optional-packages) aren't included, so SASL/GSSAPI is
  unavailable, and Snappy and LZ4 use the built-in pure-JS codecs.
- `kafka studio` in the `kafka` binary can't load the studio and prints the npm install hint. Run
  the `kafka-studio` binary instead.
- Bun's own `.env` and `bunfig.toml` autoloading is turned off, so a `.env.local` or `bunfig.toml`
  in the working directory has no effect. Config files and `.env` load as in the npm packages.
- `KAFKA_STUDIO_DEV`, the studio's Vite development mode, isn't supported.

## How we test

CI runs every package's unit test suite on Node.js 24 and 26, and on Bun 1.4.x and the latest Bun
release. On each of those runtimes, a smoke test then loads the built packages the way an
application would: it imports each one, runs the `kafka` and `kafka-studio` executables,
round-trips Snappy and LZ4 through the compression workers (and checks that the process still
exits on its own), and loads a TypeScript config file.

Integration tests against real brokers run on Node.js across the supported Kafka versions. On Bun
1.4.x and the latest release, the core suite runs against Kafka 3.6 and 4.3, and the CLI and studio
suites against Kafka 4.3.

CI also compiles the standalone binaries for Linux x64 and smoke-tests them. A release compiles
every target and runs each one on its own platform before attaching it.

See also: [Compatibility](../compatibility/) for broker versions and the implemented API surface.
