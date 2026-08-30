# @cookiemonsterdev/kafka-cli

Command-line admin client for Apache Kafka, built on
[`@cookiemonsterdev/kafka-core`](../core/README.md).

This package is still a workspace scaffold — no commands, no `bin` entry, no runtime behavior. `0.0.1`
on npm is a name-reservation release only, published ahead of schedule so npm Trusted Publishing
could be configured for this package; installing it gets you an empty package. The walking skeleton
(runtime port, argument parsing, the first commands) lands in a later track, released as `1.0.0`. See
the [kafka monorepo](https://github.com/cookieMonsterDev/kafka).

## Development

From the repo root:

```sh
pnpm --filter @cookiemonsterdev/kafka-cli build
pnpm --filter @cookiemonsterdev/kafka-cli test
pnpm --filter @cookiemonsterdev/kafka-cli typecheck
```

See the root [CONTRIBUTING.md](../../CONTRIBUTING.md) for the full workspace workflow.
