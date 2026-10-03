---
title: Introduction
description: A generic, zero-dependency config-file loader
order: 1
section: start
---

`@cookiemonsterdev/kafka-config` is a generic config-file loader: discovery, sync/async loading, a
TypeScript transform rescue, layer merging, and diagnostics. It has **zero runtime dependencies**
and no knowledge of Kafka, or of any other specific consumer.

[`@cookiemonsterdev/kafka-core`](../../../core/reference/config-file/) uses it to load
`kafka.config.ts` through `Kafka.fromConfig()` and `new Kafka()`, and the CLI and studio build on
it too. Any other consumer can use the same discovery/loading/merging machinery instead of writing
it again.

## Install

```sh
npm install @cookiemonsterdev/kafka-config
```

It runs on Node.js 24 or newer and on Bun 1.4 or newer. See
[Bun](../../../core/reference/bun/#config-loading) for how config loading differs there.

## Why a separate package

The loader machinery has no Kafka-specific code in it at all — file discovery, `require()`/
`import()` handling, the TypeScript transform rescue, and layer merging are all generic. Shipping
it as its own package means a fix or improvement reaches every consumer through a normal version
bump, instead of requiring a new release of each package that happens to use it.

Next: [API reference](../../reference/api/).
