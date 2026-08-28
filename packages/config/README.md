# @cookiemonsterdev/kafka-config

Generic config-file loader: discovery, sync/async loading, a TypeScript transform rescue, layer
merging, and diagnostics. Zero runtime dependencies.

This package is being extracted from `@cookiemonsterdev/kafka-core`'s `./config` subpath so it can
be shared by every consumer (core, the CLI, and future packages) on a plain caret range instead of
each one raising its core floor for a loader fix. See the [kafka monorepo](https://github.com/cookieMonsterDev/kafka).
