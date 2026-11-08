---
title: Upgrade between releases
description: Check installed versions, review release changes, and roll out client upgrades
order: 3
section: migration
---

This page covers upgrading this library. Use [Migrating from KafkaJS](../from-kafkajs/)
when replacing a different client. [Client differences](../breaking-changes/) describes persistent
compatibility differences rather than a chronological release history.

## Identify your current and target versions

```sh
npm ls @cookiemonsterdev/kafka-core @cookiemonsterdev/kafka-config @cookiemonsterdev/kafka-cli @cookiemonsterdev/kafka-studio
```

Read the release notes for every intervening release. Packages have independent releases:
[Core](https://github.com/cookieMonsterDev/kafka/releases?q=core-v&expanded=true),
[Config](https://github.com/cookieMonsterDev/kafka/releases?q=config-v&expanded=true),
[CLI](https://github.com/cookieMonsterDev/kafka/releases?q=cli-v&expanded=true), and
[Studio](https://github.com/cookieMonsterDev/kafka/releases?q=studio-v&expanded=true).
Use the installed package's bundled skills when they differ from the latest website.

## Before changing the dependency

Record broker and runtime versions, partitioner selection, isolation, idempotence, and timeout
settings. Review [Compatibility](../../reference/compatibility/) and changes to defaults in release
notes. Pin the desired package version in your lockfile and compile your application.

## Verify before rollout

Exercise send/consume, offset commits, shutdown, authentication, and your error-recovery path
against a staging cluster. For a change affecting partitioning or consumer group protocol,
plan a coordinated rollout rather than assuming old and new processes are interchangeable.
Check lag, retries, and error rates during deployment. Retain the previous lockfile and config
for rollback; offset changes and external side effects require their own recovery plan.

## Version-specific before and after

The removed partitioner alias is one concrete migration:

```ts
// Before: Partitioners.JavaCompatiblePartitioner
// After:
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
```

This is a fragment; import `Partitioners` from the package root. Only apply it if your old
release used the removed alias. See [Client differences](../breaking-changes/#javacompatiblepartitioner-removed).
