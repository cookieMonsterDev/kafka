# GSSAPI and delegation tokens

## GSSAPI / Kerberos

Opt-in. The broker must advertise `GSSAPI`, and the client needs a reachable KDC plus a ticket
(`kinit`) or a keytab. `serviceName` defaults to `kafka` (broker property
`sasl.kerberos.service.name`).

```ts
import { Kafka } from '@cookiemonsterdev/kafka-core';

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

Without `gssProvider`, the client loads the optional `kerberos` package (version 7 or newer).
Install it next to the client:

```sh
npm install kerberos
```

`keytab` and `krb5` are applied through process-wide environment variables during each GSS round.
Do not run concurrent GSSAPI clients with different keytabs in one process. To drive GSS tokens
from another binding, pass `gssProvider: async ({ serverToken, host, serviceName }) => ({ token,
complete })` instead.

## Delegation tokens

An Admin connected over SASL can mint a token (the broker must set
`delegation.token.secret.key`). Workers then log in with SCRAM, using the token id as the
username and the HMAC as the password:

```ts
import { Kafka } from '@cookiemonsterdev/kafka-core';

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
  sasl: { mechanism: 'scram-sha-256', tokenId: token.tokenId, tokenHmac: token.hmac },
});
```

The handshake mechanism stays `SCRAM-SHA-256` or `SCRAM-SHA-512`; there is no separate `TOKEN`
mechanism. Over PLAINTEXT or one-way SSL the broker rejects token requests with
`DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` or `DELEGATION_TOKEN_AUTH_DISABLED`.
