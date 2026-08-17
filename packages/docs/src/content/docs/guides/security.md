---
title: Security
description: TLS and SASL PLAIN, SCRAM, and OAUTHBEARER
order: 5
section: guides
---

```ts
import { Kafka } from '@cookiemonsterdev/kafka-core';

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9093'],
  ssl: true,
  sasl: { mechanism: 'plain', username: 'alice', password: 'secret' },
});
```

`ssl: true` uses Node TLS defaults. Pass a `tls.ConnectionOptions` object for
certs. See
[SSL](https://kafka.apache.org/43/security/encryption-and-authentication-using-ssl/)
and
[SASL](https://kafka.apache.org/43/security/authentication-using-sasl/).
Source:
[`types/index.ts`](https://github.com/cookieMonsterDev/kafka/blob/master/packages/core/src/types/index.ts)
(`KafkaConfig.ssl` / `sasl`).

## SASL

| `mechanism`     | Notes                                   |
| --------------- | --------------------------------------- |
| `plain`         | Username and password                   |
| `scram-sha-256` | SCRAM                                   |
| `scram-sha-512` | SCRAM                                   |
| `oauthbearer`   | Token provider                          |
| custom provider | `{ mechanism, authenticationProvider }` |

GSSAPI / Kerberos is not implemented. `aws` is an extra (non-Apache) helper.
Failed SASL throws `KafkaSASLAuthenticationError` (non-retriable). See
[Errors](../reference/errors/).
