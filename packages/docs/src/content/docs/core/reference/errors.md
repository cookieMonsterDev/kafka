---
title: Errors
sidebarLabel: Error catalog
description: Public error classes and broker protocol codes
order: 6
section: reference
---

Guide: [Errors](../../guides/errors/). Source:
[`errors.ts`](https://github.com/cookieMonsterDev/kafka/blob/master/packages/core/src/errors.ts).
Broker codes:
[`protocol/error-codes.ts`](https://github.com/cookieMonsterDev/kafka/blob/master/packages/core/src/protocol/error-codes.ts)
and the [protocol guide](https://kafka.apache.org/43/design/protocol/).

`isRebalancing` and `isKafkaError` exist in `errors.ts` but are **not** on the
public barrel (`src/index.ts`). Catch with `instanceof`.

## Hierarchy

```
Error
├── KafkaError                    retriable defaults to true
│   ├── KafkaNonRetriableError    retriable is always false
│   │   ├── KafkaServerDoesNotSupportApiKey
│   │   ├── KafkaSASLAuthenticationError
│   │   ├── KafkaPartialMessageError
│   │   ├── KafkaGroupCoordinatorNotFound
│   │   ├── KafkaNotImplemented
│   │   ├── KafkaTimeout → KafkaLockTimeout
│   │   ├── KafkaUnsupportedMagicByteInMessageSet
│   │   ├── KafkaInvariantViolation
│   │   ├── KafkaInvalidVarIntError / KafkaInvalidLongError
│   │   └── KafkaNumberOfRetriesExceeded
│   ├── KafkaProtocolError        type, code, topic?, partition?
│   │   ├── KafkaOffsetOutOfRange
│   │   ├── KafkaMemberIdRequired
│   │   ├── KafkaCreateTopicError
│   │   ├── KafkaAlterPartitionReassignmentsError
│   │   └── KafkaUpdateFeaturesError
│   ├── KafkaConnectionError → KafkaConnectionClosedError
│   ├── KafkaRequestTimeoutError
│   ├── KafkaMetadataNotLoaded → KafkaTopicMetadataNotLoaded
│   ├── KafkaStaleTopicMetadataAssignment
│   ├── KafkaDeleteGroupsError
│   ├── KafkaBrokerNotFound
│   ├── KafkaDeleteTopicRecordsError
│   └── KafkaNoBrokerAvailableError
├── KafkaAggregateError           errors: readonly unknown[]
└── KafkaFetcherRebalanceError
```

Every `KafkaError` has `name`, `retriable`, optional `helpUrl`, and `cause`.

## When they fire

| Class                             | Typical cause                                      |
| --------------------------------- | -------------------------------------------------- |
| `KafkaProtocolError`              | Broker returned an error code                      |
| `KafkaOffsetOutOfRange`           | Fetch offset not in log                            |
| `KafkaConnectionError`            | Socket / broker unreachable                        |
| `KafkaRequestTimeoutError`        | In-flight request exceeded `requestTimeout`        |
| `KafkaSASLAuthenticationError`    | SASL handshake failed                              |
| `KafkaServerDoesNotSupportApiKey` | Broker `ApiVersions` has no overlap for a used API |
| `KafkaNumberOfRetriesExceeded`    | Retrier exhausted (`retryCount`, `retryTime`)      |
| `KafkaCreateTopicError`           | CreateTopics failed for that topic name            |
| `KafkaUpdateFeaturesError`        | UpdateFeatures failed for that feature name        |
| `KafkaNoBrokerAvailableError`     | Pool has no connected broker                       |

## Protocol codes (common)

`KafkaProtocolError.type` / `.code` match these entries. `retriable` on the
class follows the table.

| Code | Type                         | Retriable | Meaning                             |
| ---- | ---------------------------- | --------- | ----------------------------------- |
| 1    | `OFFSET_OUT_OF_RANGE`        | no        | Offset not in the log               |
| 3    | `UNKNOWN_TOPIC_OR_PARTITION` | yes       | Broker does not host this partition |
| 5    | `LEADER_NOT_AVAILABLE`       | yes       | Leader election in progress         |
| 6    | `NOT_LEADER_OR_FOLLOWER`     | yes       | Wrong replica for this request      |
| 7    | `REQUEST_TIMED_OUT`          | yes       | Broker timed out                    |
| 10   | `MESSAGE_TOO_LARGE`          | no        | Record larger than max message size |
| 16   | `NOT_ENOUGH_REPLICAS`        | yes       | ISR too small for acks              |
| 19   | `INVALID_TOPIC_EXCEPTION`    | no        | Illegal topic name                  |
| 27   | `REBALANCE_IN_PROGRESS`      | yes       | Group rebalancing                   |
| 29   | `TOPIC_AUTHORIZATION_FAILED` | no        | ACL                                 |
| 33   | `UNSUPPORTED_VERSION`        | no        | API version not supported           |

The file lists every protocol code the client knows. Prefer `error instanceof
KafkaProtocolError` and then `error.type`, not string matching on `message`.
