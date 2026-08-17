---
title: Testing
description: Run unit and integration tests against a broker version
order: 6
section: guides
---

Unit tests do not start Docker. Integration tests pick a compose file from
`KAFKA_VERSION` (default `4.0`):

```sh
pnpm --filter @kafka/core test
KAFKA_VERSION=0.10 pnpm --filter @kafka/core test:integration
KAFKA_VERSION=4.0 pnpm --filter @kafka/core test:integration
KAFKA_VERSION=4.3 pnpm --filter @kafka/core test:integration
```

Leave a cluster running with `DO_NOT_STOP=1`, or point at an already-running
cluster with `KAFKA_EXTERNAL=1`. The mapping lives in
[`packages/core/test/assets/README.md`](https://github.com/cookieMonsterDev/kafka/blob/master/packages/core/test/assets/README.md).

The tested broker matrix is on [Compatibility](../reference/compatibility/).
