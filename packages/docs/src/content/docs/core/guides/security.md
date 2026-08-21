---
title: Security
description: TLS and SASL PLAIN, SCRAM, OAUTHBEARER, and delegation tokens
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

| `mechanism`     | Notes                                                  |
| --------------- | ------------------------------------------------------ |
| `plain`         | Username and password                                  |
| `scram-sha-256` | SCRAM, or delegation token via `tokenId` / `tokenHmac` |
| `scram-sha-512` | SCRAM, or delegation token via `tokenId` / `tokenHmac` |
| `oauthbearer`   | Token provider                                         |
| custom provider | `{ mechanism, authenticationProvider }`                |

GSSAPI / Kerberos is not implemented. `aws` is an extra (non-Apache) helper.
Failed SASL throws `KafkaSASLAuthenticationError` (non-retriable). See
[Errors](../reference/errors/).

## Delegation tokens

Admin `createDelegationToken` / `describeDelegationToken` /
`renewDelegationToken` / `expireDelegationToken` talk to the controller
(keys 38–41). The broker must set `delegation.token.secret.key` and the
client must use SASL — not PLAINTEXT or one-way SSL, or the broker returns
`DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` / `DELEGATION_TOKEN_AUTH_DISABLED`.
Default test compose files do not enable tokens.

Logging in with a minted token piggybacks on SASL/SCRAM (not a separate
`TOKEN` mechanism). `tokenId` is the SCRAM username, `tokenHmac` is the
password (a `Buffer` from Admin is encoded as standard base64), and the
client-first message includes `tokenauth=true` so the broker looks up the
token instead of a stored SCRAM user. The handshake mechanism stays
`SCRAM-SHA-256` or `SCRAM-SHA-512`. The broker must enable that SCRAM
mechanism and `delegation.token.secret.key`. See
[SASL authentication](https://kafka.apache.org/43/security/authentication-using-sasl/).

```ts
const adminKafka = new Kafka({
  brokers: ['localhost:9093'],
  ssl: true,
  sasl: { mechanism: 'scram-sha-256', username: 'alice', password: 'alice-secret' },
});
const admin = adminKafka.admin();
await admin.connect();
const token = await admin.createDelegationToken();
await admin.disconnect();

const worker = new Kafka({
  brokers: ['localhost:9093'],
  ssl: true,
  sasl: {
    mechanism: 'scram-sha-256',
    tokenId: token.tokenId,
    tokenHmac: token.hmac,
  },
});
const producer = worker.producer();
await producer.connect();
```
