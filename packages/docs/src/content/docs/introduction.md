---
title: Introduction
description: A TypeScript Kafka client for Kafka 3.x/4.x
order: 1
---

`@kafka/core` is a TypeScript Kafka client with a kafkajs-shaped public API:
`new Kafka({ brokers }).producer() / consumer() / admin()`.

It targets **Kafka 3.x/4.x (KRaft only)**. Offsets are `bigint`, ZSTD is built
in, and types are generated from source rather than hand-maintained.

This site documents the public API and the differences versus kafkajs 2.2.4.
The library lives in `packages/core`; this Astro site is `packages/docs`.
