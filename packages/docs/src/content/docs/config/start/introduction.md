---
title: Introduction
description: A generic, zero-dependency config-file loader
order: 1
section: start
---

`@cookiemonsterdev/kafka-config` is a generic config-file loader: discovery, sync/async loading, a
TypeScript transform rescue, layer merging, and diagnostics. It has **zero runtime dependencies**
and no knowledge of Kafka, or of any other specific consumer.

`@cookiemonsterdev/kafka-core` doesn't read a config file yet — `new Kafka({...})` still takes its
options directly. This package exists so that when it does (and so any other consumer that wants
to), the discovery/loading/merging machinery doesn't need to be written twice.

## Install

```sh
npm install @cookiemonsterdev/kafka-config
```

## Why a separate package

The loader machinery has no Kafka-specific code in it at all — file discovery, `require()`/
`import()` handling, the TypeScript transform rescue, and layer merging are all generic. Shipping
it as its own package means a fix or improvement reaches every consumer through a normal version
bump, instead of requiring a new release of each package that happens to use it.

Next: [API reference](../../reference/api/).
