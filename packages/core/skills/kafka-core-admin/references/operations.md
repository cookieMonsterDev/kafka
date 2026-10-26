# Admin operations

All snippets assume a connected `admin` from `kafka.admin()`.

## ACLs

```ts
import {
  AclOperationTypes,
  AclPermissionTypes,
  AclResourceTypes,
  Kafka,
  ResourcePatternTypes,
} from '@cookiemonsterdev/kafka-core';

const admin = new Kafka({ brokers: ['localhost:9092'] }).admin();
await admin.connect();

await admin.createAcls({
  acl: [
    {
      resourceType: AclResourceTypes.TOPIC,
      resourceName: 'events',
      resourcePatternType: ResourcePatternTypes.LITERAL,
      principal: 'User:bob',
      host: '*',
      operation: AclOperationTypes.READ,
      permissionType: AclPermissionTypes.ALLOW,
    },
  ],
});
```

`describeAcls(filter)` and `deleteAcls({ filters })` take the same shape as a filter. The broker
must have an authorizer configured.

## SCRAM credentials

`describeUserScramCredentials` / `alterUserScramCredentials({ upsertions, deletions })`. Upserting
with a plain `password` lets the client salt and hash it. Kafka 2.7+.

## Delegation tokens

`createDelegationToken`, `describeDelegationToken`, `renewDelegationToken({ hmac,
renewTimePeriodMs })`, `expireDelegationToken({ hmac, expiryTimePeriodMs })`. The connection must
be SASL and the broker must set `delegation.token.secret.key`. HMACs are `Buffer`; durations and
timestamps are `bigint`. Logging in with a token is covered in the `kafka-core-setup` skill.

## Transactions (Kafka 3.0+)

- `describeTransactions(transactionalIds)` and `listTransactions(options?)` inspect state.
- `describeProducers({ topicPartitions })` shows active producers per partition.
- `fenceProducers` / `forceTerminateTransaction` (Kafka 2.5+) fence a transactional id.
- `abortTransaction` writes an abort marker for a hanging transaction.

Fencing and aborting break the owning application's in-flight transaction; coordinate first.

## Finalized features

```ts
import { FeatureUpdateUpgradeTypes } from '@cookiemonsterdev/kafka-core';

await admin.updateFeatures({
  featureUpdates: [
    { feature: 'metadata.version', maxVersionLevel: 20, upgradeType: FeatureUpdateUpgradeTypes.UPGRADE },
  ],
  validateOnly: true,
});
```

Always run with `validateOnly: true` first. `describeFeatures()` reads the current levels.

## KRaft controller bootstrap (Kafka 3.7+)

When brokers are down, point an admin at the controller quorum directly:

```ts
const controllerAdmin = kafka.admin({ bootstrapControllers: ['localhost:9093'] });
await controllerAdmin.connect();
await controllerAdmin.describeMetadataQuorum();
```

Only controller-targeted methods (quorum, voters, features, `unregisterBroker`) work this way;
topic metadata and produce/consume still need brokers.
