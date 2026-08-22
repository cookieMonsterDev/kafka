---
title: Security
description: TLS and SASL PLAIN, SCRAM, OAUTHBEARER, GSSAPI, and delegation tokens
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
| `gssapi`        | Kerberos; `gssProvider` or optional `kerberos` package |
| custom provider | `{ mechanism, authenticationProvider }`                |

`aws` is an extra (non-Apache) helper. Failed SASL throws
`KafkaSASLAuthenticationError` (non-retriable). See
[Errors](../../reference/errors/).

## GSSAPI / Kerberos

Opt-in. Handshake mechanism name is `GSSAPI`. `serviceName` defaults to
`kafka` (Java `sasl.kerberos.service.name`). The broker must advertise GSSAPI
and you need a reachable KDC plus a ticket or keytab.

```ts
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['broker.example.com:9093'],
  ssl: true,
  sasl: {
    mechanism: 'gssapi',
    serviceName: 'kafka',
    principal: 'alice@EXAMPLE.COM',
    keytab: '/etc/security/keytabs/alice.keytab',
    krb5: '/etc/krb5.conf',
  },
});
```

Without `gssProvider`, the client loads the optional [`kerberos`](https://www.npmjs.com/package/kerberos)
package (`>=7`, Node 20+ prebuilds including Node 24) and runs GSS token
exchange plus RFC 4752 wrap. Install it next to the client:

```sh
npm install kerberos
```

`keytab` / `krb5` are applied as `KRB5_CLIENT_KTNAME` / `KRB5_KTNAME` and
`KRB5_CONFIG` for the duration of each GSS round (process-wide; do not run
concurrent GSSAPI clients with different keytabs in one process). You can
instead `kinit` and omit those fields.

To drive tokens yourself (another binding, or tokens from `kinit` + a GSS
library):

```ts
sasl: {
  mechanism: 'gssapi',
  serviceName: 'kafka',
  gssProvider: async ({ serverToken, host, serviceName }) => {
    // Return the next client GSS token. Set complete after the last send
    // (including the RFC 4752 wrap of the authorization identity).
    return { token: nextToken(serverToken, host, serviceName), complete: false };
  },
}
```

CI does not run a Kerberos KDC. Unit tests mock the token exchange. A manual
harness is: MIT krb5 KDC + Kafka `sasl.enabled.mechanisms=GSSAPI` + a client
principal/keytab, then `kinit` (or `keytab` / `krb5` as above) and connect.

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
