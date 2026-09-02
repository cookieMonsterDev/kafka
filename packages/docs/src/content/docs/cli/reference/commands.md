---
title: Command reference
description: Every kafka command, generated from the command registry
order: 1
section: reference
---

_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not
edit by hand; run that script again after changing a command._

## General

### completion

```sh
kafka completion <shell>
```

Print a shell completion script for bash, zsh, or fish

Examples:

```sh
kafka completion bash
kafka completion zsh
kafka completion fish
```

### doctor

```sh
kafka doctor
```

Report where connection settings come from — config file, environment, profile, or flag

| Flag                 | Description                                          |
| -------------------- | ---------------------------------------------------- |
| `--brokers <string>` | comma-separated broker list, to diagnose as if given |

Examples:

```sh
kafka doctor
kafka doctor --profile staging
```

### init

```sh
kafka init
```

Scaffold a kafka.config file in the current directory

| Flag      | Description                                                    |
| --------- | -------------------------------------------------------------- |
| `--ts`    | scaffold kafka.config.ts (default when TypeScript is detected) |
| `--js`    | scaffold kafka.config.mjs instead                              |
| `--force` | overwrite the file if it already exists                        |

Examples:

```sh
kafka init
kafka init --js
kafka init --force
```

### ping

```sh
kafka ping
```

Check connectivity to the cluster

| Flag                 | Description                                      |
| -------------------- | ------------------------------------------------ |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092 |

Examples:

```sh
kafka ping --brokers localhost:9092
```

### profiles

```sh
kafka profiles
```

List the named connection profiles configured under cli.profiles

Examples:

```sh
kafka profiles
```

## acl

### acl add

```sh
kafka acl add <principals...>
```

Create one or more ACLs, granting or denying an operation

| Flag                                | Description                                                              |
| ----------------------------------- | ------------------------------------------------------------------------ |
| `--brokers <string>`                | comma-separated broker list, e.g. localhost:9092                         |
| `--resource-type <string>`          | resource type: topic, group, cluster, transactional-id, delegation-token |
| `--resource-name <string>`          | exact name of the resource                                               |
| `--pattern-type <string>`           | resource pattern type: literal or prefixed (default: literal)            |
| `--operation <string> (repeatable)` | operation to grant (repeatable), e.g. read, write                        |
| `--host <string>`                   | host the principal connects from (default: *)                            |
| `--permission-type <string>`        | allow or deny (default: allow)                                           |
| `--dry-run`                         | print what would be created without creating anything                    |

Examples:

```sh
kafka acl add User:alice --resource-type topic --resource-name orders --operation read --operation write
kafka acl add User:alice User:bob --resource-type group --resource-name my-group --operation read --dry-run
```

### acl list

```sh
kafka acl list
```

List ACLs matching a filter, or every ACL by default

| Flag                         | Description                                        |
| ---------------------------- | -------------------------------------------------- |
| `--brokers <string>`         | comma-separated broker list, e.g. localhost:9092   |
| `--resource-type <string>`   | limit to this resource type (default: any)         |
| `--resource-name <string>`   | limit to this exact resource name                  |
| `--pattern-type <string>`    | limit to this resource pattern type (default: any) |
| `--principal <string>`       | limit to this principal, e.g. User:alice           |
| `--host <string>`            | limit to this host                                 |
| `--operation <string>`       | limit to this operation (default: any)             |
| `--permission-type <string>` | limit to this permission type (default: any)       |

Examples:

```sh
kafka acl list --brokers localhost:9092
kafka acl list --resource-type topic --resource-name orders
```

### acl remove

```sh
kafka acl remove <principals...>
```

Delete every ACL matching a filter, per principal

| Flag                         | Description                                        |
| ---------------------------- | -------------------------------------------------- |
| `--brokers <string>`         | comma-separated broker list, e.g. localhost:9092   |
| `--resource-type <string>`   | limit to this resource type (default: any)         |
| `--resource-name <string>`   | limit to this exact resource name                  |
| `--pattern-type <string>`    | limit to this resource pattern type (default: any) |
| `--host <string>`            | limit to this host                                 |
| `--operation <string>`       | limit to this operation (default: any)             |
| `--permission-type <string>` | limit to this permission type (default: any)       |
| `--yes`                      | confirm the deletion without an interactive prompt |

Examples:

```sh
kafka acl remove User:alice --resource-type topic --resource-name orders --brokers localhost:9092 --yes
```

## admin

### admin call

```sh
kafka admin call <method>
```

Call any Admin method by name — the escape hatch for everything without its own command _(unstable)_

| Flag                   | Description                                                           |
| ---------------------- | --------------------------------------------------------------------- |
| `--brokers <string>`   | comma-separated broker list, e.g. localhost:9092                      |
| `--from-file <string>` | path to a JSON file with the method arguments                         |
| `--yes`                | confirm a non-read-only method                                        |
| `--force`              | confirm a non-read-only method (required alongside --yes)             |
| `--show-secrets`       | print credential fields (password, hmac, …) instead of redacting them |

Examples:

```sh
kafka admin call listTopics --brokers localhost:9092
kafka admin call createAcls --from-file ./acls.json --yes --force
```

### admin methods

```sh
kafka admin methods
```

List every Admin method admin call can reach, and how each is classified

Examples:

```sh
kafka admin methods
```

## cluster

### cluster elect-leaders

```sh
kafka cluster elect-leaders
```

Trigger a preferred or unclean leader election on one or more partitions

| Flag                                      | Description                                                      |
| ----------------------------------------- | ---------------------------------------------------------------- |
| `--brokers <string>`                      | comma-separated broker list, e.g. localhost:9092                 |
| `--election-type <string>`                | preferred or unclean                                             |
| `--topic-partition <string> (repeatable)` | a "topic:partition" to elect a leader on (repeatable)            |
| `--all-topic-partitions`                  | elect on every eligible partition                                |
| `--from-file <string>`                    | path to a kafka-leader-election.sh --path-to-json-file JSON file |
| `--timeout <number>`                      | request timeout in ms                                            |
| `--dry-run`                               | print the election target and exit without connecting            |
| `--yes`                                   | confirm the election without an interactive prompt               |
| `--force`                                 | required for --election-type unclean, which can lose data        |

Examples:

```sh
kafka cluster elect-leaders --election-type preferred --all-topic-partitions --brokers localhost:9092 --yes
kafka cluster elect-leaders --election-type unclean --topic-partition orders:0 --force --yes --brokers localhost:9092
```

### cluster features

```sh
kafka cluster features
```

Describe supported and finalized feature versions

| Flag                 | Description                                      |
| -------------------- | ------------------------------------------------ |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092 |

Examples:

```sh
kafka cluster features --brokers localhost:9092
```

### cluster info

```sh
kafka cluster info
```

Describe the cluster: its brokers, controller, and cluster id

| Flag                 | Description                                      |
| -------------------- | ------------------------------------------------ |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092 |

Examples:

```sh
kafka cluster info --brokers localhost:9092
```

### cluster log-dirs

```sh
kafka cluster log-dirs
```

Describe log directories and their partition sizes, per broker

| Flag                             | Description                                        |
| -------------------------------- | -------------------------------------------------- |
| `--brokers <string>`             | comma-separated broker list, e.g. localhost:9092   |
| `--broker <number> (repeatable)` | limit to this broker id (repeatable; default: all) |
| `--topic <string> (repeatable)`  | limit to this topic (repeatable; default: all)     |

Examples:

```sh
kafka cluster log-dirs --brokers localhost:9092
kafka cluster log-dirs --topic orders --broker 1 --broker 2
```

### cluster quorum

```sh
kafka cluster quorum
```

Describe the metadata quorum: leader, voters, and observers

| Flag                 | Description                                      |
| -------------------- | ------------------------------------------------ |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092 |

Examples:

```sh
kafka cluster quorum --brokers localhost:9092
```

### cluster raft-voter add

```sh
kafka cluster raft-voter add
```

Add a voter to the KRaft metadata quorum

| Flag                               | Description                                                       |
| ---------------------------------- | ----------------------------------------------------------------- |
| `--brokers <string>`               | comma-separated broker list, e.g. localhost:9092                  |
| `--voter-id <number>`              | the new voter’s node id                                           |
| `--voter-directory-id <string>`    | the new voter’s directory id, as a uuid                           |
| `--listener <string> (repeatable)` | a listener the voter is reachable on, name=host:port (repeatable) |
| `--cluster-id <string>`            | expected cluster id, if any                                       |
| `--timeout-ms <number>`            | request timeout in ms                                             |
| `--ack-when-committed`             | wait for the addition to be committed before returning            |
| `--dry-run`                        | print the voter that would be added and exit without connecting   |

Examples:

```sh
kafka cluster raft-voter add --voter-id 4 --voter-directory-id 3c48b... --listener CONTROLLER=localhost:9093 --brokers localhost:9092
```

### cluster raft-voter remove

```sh
kafka cluster raft-voter remove
```

Remove a voter from the KRaft metadata quorum

| Flag                            | Description                                       |
| ------------------------------- | ------------------------------------------------- |
| `--brokers <string>`            | comma-separated broker list, e.g. localhost:9092  |
| `--voter-id <number>`           | the voter’s node id                               |
| `--voter-directory-id <string>` | the voter’s directory id, as a uuid               |
| `--cluster-id <string>`         | expected cluster id, if any                       |
| `--yes`                         | confirm the removal without an interactive prompt |

Examples:

```sh
kafka cluster raft-voter remove --voter-id 4 --voter-directory-id 3c48b... --brokers localhost:9092 --yes
```

### cluster reassign execute

```sh
kafka cluster reassign execute
```

Execute a partition reassignment from a kafka-reassign-partitions.sh-shaped JSON file

| Flag                   | Description                                                                            |
| ---------------------- | -------------------------------------------------------------------------------------- |
| `--brokers <string>`   | comma-separated broker list, e.g. localhost:9092                                       |
| `--from-file <string>` | path to a JSON file in the kafka-reassign-partitions.sh --reassignment-json-file shape |
| `--timeout <number>`   | request timeout in ms                                                                  |
| `--dry-run`            | print the planned reassignment and exit without connecting                             |
| `--yes`                | confirm the reassignment without an interactive prompt                                 |

Examples:

```sh
kafka cluster reassign execute --from-file reassignment.json --brokers localhost:9092 --yes
```

### cluster reassign list

```sh
kafka cluster reassign list
```

List every active partition reassignment

| Flag                 | Description                                      |
| -------------------- | ------------------------------------------------ |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092 |
| `--timeout <number>` | request timeout in ms                            |

Examples:

```sh
kafka cluster reassign list --brokers localhost:9092
```

### cluster unregister-broker

```sh
kafka cluster unregister-broker
```

Unregister a broker from the cluster (KRaft broker decommissioning)

| Flag                   | Description                                              |
| ---------------------- | -------------------------------------------------------- |
| `--brokers <string>`   | comma-separated broker list, e.g. localhost:9092         |
| `--broker-id <number>` | id of the broker to unregister                           |
| `--yes`                | confirm the unregistration without an interactive prompt |

Examples:

```sh
kafka cluster unregister-broker --broker-id 3 --brokers localhost:9092 --yes
```

### cluster update-features

```sh
kafka cluster update-features
```

Upgrade, safe-downgrade, or unsafe-downgrade one or more finalized feature versions

| Flag                                 | Description                                                                                 |
| ------------------------------------ | ------------------------------------------------------------------------------------------- |
| `--brokers <string>`                 | comma-separated broker list, e.g. localhost:9092                                            |
| `--feature <key=value> (repeatable)` | a feature to update, name=maxVersionLevel (repeatable)                                      |
| `--upgrade-type <string>`            | upgrade, safe-downgrade, or unsafe-downgrade (default: upgrade), applied to every --feature |
| `--timeout <number>`                 | request timeout in ms                                                                       |
| `--dry-run`                          | validate on the broker without finalizing anything                                          |
| `--yes`                              | confirm the update without an interactive prompt                                            |
| `--force`                            | required in addition to --yes for an unsafe downgrade                                       |

Examples:

```sh
kafka cluster update-features --feature kraft.version=1 --brokers localhost:9092 --yes
kafka cluster update-features --feature kraft.version=0 --upgrade-type unsafe-downgrade --yes --force --brokers localhost:9092
```

## config

### config describe

```sh
kafka config describe <names...>
```

Describe the configs of one or more resources

| Flag                                  | Description                                                           |
| ------------------------------------- | --------------------------------------------------------------------- |
| `--brokers <string>`                  | comma-separated broker list, e.g. localhost:9092                      |
| `--type <string>`                     | resource type: topic, broker, broker-logger, client-metrics, or group |
| `--config-name <string> (repeatable)` | limit the result to this config key (repeatable, default: every key)  |
| `--include-synonyms`                  | include each entry's config synonyms                                  |
| `--include-documentation`             | include each entry's documentation string                             |
| `--show-secrets`                      | print a sensitive config value instead of redacting it                |

Examples:

```sh
kafka config describe --type topic orders --brokers localhost:9092
kafka config describe --type broker 1 --include-synonyms --brokers localhost:9092
```

### config list-resources

```sh
kafka config list-resources
```

List every config resource the broker knows about

| Flag                           | Description                                                   |
| ------------------------------ | ------------------------------------------------------------- |
| `--brokers <string>`           | comma-separated broker list, e.g. localhost:9092              |
| `--type <string> (repeatable)` | limit to this resource type (repeatable, default: every type) |

Examples:

```sh
kafka config list-resources --brokers localhost:9092
kafka config list-resources --type topic --type group
```

### config set

```sh
kafka config set <names...>
```

Set one or more config entries on a resource

| Flag                               | Description                                                           |
| ---------------------------------- | --------------------------------------------------------------------- |
| `--brokers <string>`               | comma-separated broker list, e.g. localhost:9092                      |
| `--type <string>`                  | resource type: topic, broker, broker-logger, client-metrics, or group |
| `--entry <key=value> (repeatable)` | a config entry to set, key=value (repeatable)                         |
| `--dry-run`                        | validate without changing anything                                    |

Examples:

```sh
kafka config set --type topic orders --entry retention.ms=604800000
kafka config set --type topic orders payments --entry cleanup.policy=compact --dry-run
```

### config unset

```sh
kafka config unset <names...>
```

Remove one or more config entries from a resource, reverting them to default

| Flag                          | Description                                                           |
| ----------------------------- | --------------------------------------------------------------------- |
| `--brokers <string>`          | comma-separated broker list, e.g. localhost:9092                      |
| `--type <string>`             | resource type: topic, broker, broker-logger, client-metrics, or group |
| `--key <string> (repeatable)` | a config key to remove (repeatable)                                   |
| `--dry-run`                   | validate without changing anything                                    |

Examples:

```sh
kafka config unset --type topic orders --key retention.ms
kafka config unset --type topic orders payments --key cleanup.policy --dry-run
```

## group

### group delete

```sh
kafka group delete <groupIds...>
```

Delete one or more consumer groups

| Flag                 | Description                                        |
| -------------------- | -------------------------------------------------- |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092   |
| `--yes`              | confirm the deletion without an interactive prompt |

Examples:

```sh
kafka group delete my-group --brokers localhost:9092 --yes
```

### group delete-offsets

```sh
kafka group delete-offsets <groupId>
```

Delete a consumer group's committed offsets on one or more topics

| Flag                                | Description                                                                                          |
| ----------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `--brokers <string>`                | comma-separated broker list, e.g. localhost:9092                                                     |
| `--yes`                             | confirm the deletion without an interactive prompt                                                   |
| `--topic <string> (repeatable)`     | topic to clear (repeatable; at least one required)                                                   |
| `--partition <number> (repeatable)` | partition to clear, applied to every --topic (repeatable; defaults to every partition of each topic) |

Examples:

```sh
kafka group delete-offsets my-group --topic orders --brokers localhost:9092 --yes
```

### group describe

```sh
kafka group describe <groupIds...>
```

Describe one or more consumer groups

| Flag                 | Description                                      |
| -------------------- | ------------------------------------------------ |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092 |

Examples:

```sh
kafka group describe my-group --brokers localhost:9092
```

### group list

```sh
kafka group list
```

List every consumer group the cluster knows about

| Flag                 | Description                                      |
| -------------------- | ------------------------------------------------ |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092 |

Examples:

```sh
kafka group list --brokers localhost:9092
```

### group offsets

```sh
kafka group offsets <groupId>
```

Show a consumer group's committed offsets

| Flag                            | Description                                                                                    |
| ------------------------------- | ---------------------------------------------------------------------------------------------- |
| `--brokers <string>`            | comma-separated broker list, e.g. localhost:9092                                               |
| `--topic <string> (repeatable)` | limit the result to this topic (repeatable; defaults to every topic the group has offsets for) |

Examples:

```sh
kafka group offsets my-group --brokers localhost:9092
kafka group offsets my-group --topic orders
```

### group remove-members

```sh
kafka group remove-members <groupId>
```

Remove one or more static members from a consumer group's session

| Flag                             | Description                                                                                |
| -------------------------------- | ------------------------------------------------------------------------------------------ |
| `--brokers <string>`             | comma-separated broker list, e.g. localhost:9092                                           |
| `--yes`                          | confirm the removal without an interactive prompt                                          |
| `--member <string> (repeatable)` | member to remove, memberId or memberId:groupInstanceId (repeatable; at least one required) |

Examples:

```sh
kafka group remove-members my-group --member consumer-1-abc --brokers localhost:9092 --yes
```

### group reset-offsets

```sh
kafka group reset-offsets <groupId>
```

Reset a consumer group's committed offsets on one or more topics

| Flag                            | Description                                           |
| ------------------------------- | ----------------------------------------------------- |
| `--brokers <string>`            | comma-separated broker list, e.g. localhost:9092      |
| `--topic <string> (repeatable)` | topic to reset (repeatable; at least one required)    |
| `--to <string>`                 | reset target: earliest or latest                      |
| `--execute`                     | actually reset the offsets (without it, preview only) |
| `--yes`                         | confirm the reset without an interactive prompt       |

Examples:

```sh
kafka group reset-offsets my-group --topic orders --to earliest --brokers localhost:9092
kafka group reset-offsets my-group --topic orders --to earliest --execute --yes --brokers localhost:9092
```

## quota

### quota alter

```sh
kafka quota alter
```

Set or remove client quota values for one entity

| Flag                                | Description                                                                 |
| ----------------------------------- | --------------------------------------------------------------------------- |
| `--brokers <string>`                | comma-separated broker list, e.g. localhost:9092                            |
| `--entity <key=value> (repeatable)` | an entity type=name to alter, or type= for its cluster default (repeatable) |
| `--set <key=value> (repeatable)`    | a quota key=value to set (repeatable)                                       |
| `--unset <string> (repeatable)`     | a quota key to remove (repeatable)                                          |
| `--dry-run`                         | validate without changing anything                                          |

Examples:

```sh
kafka quota alter --entity user=alice --set producer_byte_rate=1048576 --brokers localhost:9092
kafka quota alter --entity user=alice --entity client-id=orders-producer --unset producer_byte_rate --brokers localhost:9092
```

### quota describe

```sh
kafka quota describe
```

Describe client quotas matching an entity filter

| Flag                                 | Description                                                                         |
| ------------------------------------ | ----------------------------------------------------------------------------------- |
| `--brokers <string>`                 | comma-separated broker list, e.g. localhost:9092                                    |
| `--entity <key=value> (repeatable)`  | an entity type=name to match exactly, or type= for its cluster default (repeatable) |
| `--entity-any <string> (repeatable)` | an entity type to match with any specified name (repeatable)                        |
| `--strict`                           | reject entity types with no filter component                                        |

Examples:

```sh
kafka quota describe --entity user=alice --brokers localhost:9092
kafka quota describe --entity-any client-id --brokers localhost:9092
```

## scram

### scram delete

```sh
kafka scram delete <users...>
```

Delete a SCRAM credential for one or more users

| Flag                   | Description                                        |
| ---------------------- | -------------------------------------------------- |
| `--brokers <string>`   | comma-separated broker list, e.g. localhost:9092   |
| `--mechanism <string>` | scram-sha-256 or scram-sha-512                     |
| `--yes`                | confirm the deletion without an interactive prompt |

Examples:

```sh
kafka scram delete alice --mechanism scram-sha-256 --brokers localhost:9092 --yes
```

### scram list

```sh
kafka scram list <users...>
```

List SCRAM credentials for one or more users, or every user

| Flag                 | Description                                      |
| -------------------- | ------------------------------------------------ |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092 |

Examples:

```sh
kafka scram list --brokers localhost:9092
kafka scram list alice bob --brokers localhost:9092
```

### scram set

```sh
kafka scram set <users...>
```

Create or update a SCRAM credential for one or more users

| Flag                    | Description                                                   |
| ----------------------- | ------------------------------------------------------------- |
| `--brokers <string>`    | comma-separated broker list, e.g. localhost:9092              |
| `--mechanism <string>`  | scram-sha-256 or scram-sha-512                                |
| `--iterations <number>` | PBKDF2 iteration count (defaults to 4096, the broker minimum) |
| `--password-stdin`      | read the password from stdin — never accepted as a plain flag |

Examples:

```sh
kafka scram set alice --mechanism scram-sha-256 --password-stdin --brokers localhost:9092
```

## share-group

### share-group delete

```sh
kafka share-group delete <groupIds...>
```

Delete one or more share groups

| Flag                 | Description                                        |
| -------------------- | -------------------------------------------------- |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092   |
| `--yes`              | confirm the deletion without an interactive prompt |

Examples:

```sh
kafka share-group delete orders-readers --brokers localhost:9092 --yes
```

### share-group describe

```sh
kafka share-group describe <groupIds...>
```

Describe one or more share groups

| Flag                 | Description                                      |
| -------------------- | ------------------------------------------------ |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092 |

Examples:

```sh
kafka share-group describe orders-readers --brokers localhost:9092
```

### share-group list

```sh
kafka share-group list
```

List every share group the cluster knows about

| Flag                 | Description                                      |
| -------------------- | ------------------------------------------------ |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092 |

Examples:

```sh
kafka share-group list --brokers localhost:9092
```

### share-group offsets

```sh
kafka share-group offsets <groupId>
```

Read, set, or delete a share group’s committed start offsets

| Flag                                   | Description                                                          |
| -------------------------------------- | -------------------------------------------------------------------- |
| `--brokers <string>`                   | comma-separated broker list, e.g. localhost:9092                     |
| `--topic <string> (repeatable)`        | limit a read to these topics (repeatable)                            |
| `--set <string> (repeatable)`          | "topic:partition:offset" to set as the new start offset (repeatable) |
| `--delete-topic <string> (repeatable)` | delete committed offsets for this topic (repeatable)                 |
| `--yes`                                | confirm --set/--delete-topic without an interactive prompt           |

Examples:

```sh
kafka share-group offsets orders-readers --brokers localhost:9092
kafka share-group offsets orders-readers --set orders:0:1000 --yes --brokers localhost:9092
kafka share-group offsets orders-readers --delete-topic orders --yes --brokers localhost:9092
```

## token

### token create

```sh
kafka token create
```

Create a delegation token

| Flag                              | Description                                                                 |
| --------------------------------- | --------------------------------------------------------------------------- |
| `--brokers <string>`              | comma-separated broker list, e.g. localhost:9092                            |
| `--owner <string>`                | owning principal, "PrincipalType:name" (defaults to the authenticated user) |
| `--renewer <string> (repeatable)` | a principal allowed to renew the token, "PrincipalType:name" (repeatable)   |
| `--max-life-time-ms <string>`     | maximum token lifetime in ms (defaults to the broker setting)               |
| `--show-secrets`                  | print the token hmac instead of redacting it                                |

Examples:

```sh
kafka token create --renewer User:alice --brokers localhost:9092 --show-secrets
```

### token expire

```sh
kafka token expire
```

Expire a delegation token, immediately by default

| Flag                               | Description                                                     |
| ---------------------------------- | --------------------------------------------------------------- |
| `--brokers <string>`               | comma-separated broker list, e.g. localhost:9092                |
| `--hmac <string>`                  | the token's hmac, base64 (mutually exclusive with --hmac-stdin) |
| `--hmac-stdin`                     | read the token's hmac (base64) from stdin                       |
| `--expiry-time-period-ms <string>` | ms until expiry (defaults to immediate expiry)                  |
| `--yes`                            | confirm the expiry without an interactive prompt                |

Examples:

```sh
kafka token expire --hmac-stdin --brokers localhost:9092 --yes
```

### token list

```sh
kafka token list
```

List delegation tokens, optionally filtered by owner

| Flag                            | Description                                                         |
| ------------------------------- | ------------------------------------------------------------------- |
| `--brokers <string>`            | comma-separated broker list, e.g. localhost:9092                    |
| `--owner <string> (repeatable)` | an owning principal to filter on, "PrincipalType:name" (repeatable) |
| `--show-secrets`                | print each token hmac instead of redacting it                       |

Examples:

```sh
kafka token list --brokers localhost:9092
kafka token list --owner User:alice --brokers localhost:9092
```

### token renew

```sh
kafka token renew
```

Renew a delegation token, extending its expiry

| Flag                              | Description                                                     |
| --------------------------------- | --------------------------------------------------------------- |
| `--brokers <string>`              | comma-separated broker list, e.g. localhost:9092                |
| `--hmac <string>`                 | the token's hmac, base64 (mutually exclusive with --hmac-stdin) |
| `--hmac-stdin`                    | read the token's hmac (base64) from stdin                       |
| `--renew-time-period-ms <string>` | how long to extend the expiry by, in ms                         |

Examples:

```sh
kafka token renew --hmac-stdin --renew-time-period-ms 86400000 --brokers localhost:9092
```

## topic

### topic add-partitions

```sh
kafka topic add-partitions <topics...>
```

Raise a topic to a new total partition count

| Flag                 | Description                                      |
| -------------------- | ------------------------------------------------ |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092 |
| `--count <number>`   | the new total number of partitions (not a delta) |
| `--dry-run`          | validate without changing anything               |

Examples:

```sh
kafka topic add-partitions orders --count 6 --brokers localhost:9092
kafka topic add-partitions orders payments --count 6 --dry-run
```

### topic create

```sh
kafka topic create <topics...>
```

Create one or more topics

| Flag                                         | Description                                                                                                  |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `--brokers <string>`                         | comma-separated broker list, e.g. localhost:9092                                                             |
| `-p, --partitions <number>`                  | number of partitions                                                                                         |
| `-r, --replication-factor <number>`          | replication factor                                                                                           |
| `--replica-assignment <string> (repeatable)` | explicit partition=replica,replica assignment (repeatable, exclusive with --partitions/--replication-factor) |
| `--config <key=value> (repeatable)`          | a topic config entry, key=value (repeatable)                                                                 |
| `--dry-run`                                  | validate without creating anything                                                                           |
| `--if-not-exists`                            | treat an already-existing topic as success                                                                   |
| `--fail-fast`                                | issue one batched call instead of one call per topic                                                         |

Examples:

```sh
kafka topic create orders --partitions 3 --replication-factor 1
kafka topic create orders payments --dry-run
```

### topic delete

```sh
kafka topic delete <topics...>
```

Delete one or more topics

| Flag                 | Description                                              |
| -------------------- | -------------------------------------------------------- |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092         |
| `--if-exists`        | treat a missing topic as success                         |
| `--yes`              | confirm the deletion without an interactive prompt       |
| `--force`            | override the internal-topic and batch-size safety checks |

Examples:

```sh
kafka topic delete orders --brokers localhost:9092 --yes
kafka topic delete __consumer_offsets --brokers localhost:9092 --yes --force
```

### topic delete-records

```sh
kafka topic delete-records
```

Delete records before a given offset, per partition

| Flag                   | Description                                                                 |
| ---------------------- | --------------------------------------------------------------------------- |
| `--brokers <string>`   | comma-separated broker list, e.g. localhost:9092                            |
| `--from-file <string>` | path to a JSON file in the kafka-delete-records.sh --offset-json-file shape |
| `--yes`                | confirm the deletion without an interactive prompt                          |

Examples:

```sh
kafka topic delete-records --from-file offsets.json --brokers localhost:9092 --yes
```

### topic describe

```sh
kafka topic describe <topics...>
```

Describe one or more topics

| Flag                 | Description                                      |
| -------------------- | ------------------------------------------------ |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092 |

Examples:

```sh
kafka topic describe orders --brokers localhost:9092
```

### topic list

```sh
kafka topic list
```

List every topic

| Flag                 | Description                                      |
| -------------------- | ------------------------------------------------ |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092 |

Examples:

```sh
kafka topic list --brokers localhost:9092
```

### topic offsets

```sh
kafka topic offsets <topic>
```

Show partition offsets for a topic

| Flag                 | Description                                                                             |
| -------------------- | --------------------------------------------------------------------------------------- |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092                                        |
| `--time <string>`    | resolve offsets as of "earliest", "latest", "max-timestamp", or a millisecond timestamp |

Examples:

```sh
kafka topic offsets orders --brokers localhost:9092
kafka topic offsets orders --time earliest --brokers localhost:9092
kafka topic offsets orders --time 1735689600000 --brokers localhost:9092
```

### topic producers

```sh
kafka topic producers <topic>
```

Show a topic's active producer state, per partition

| Flag                                | Description                                                        |
| ----------------------------------- | ------------------------------------------------------------------ |
| `--brokers <string>`                | comma-separated broker list, e.g. localhost:9092                   |
| `--partition <number> (repeatable)` | partition index to query (repeatable; defaults to every partition) |
| `--broker-id <string>`              | query a specific replica broker instead of the partition leaders   |

Examples:

```sh
kafka topic producers orders --brokers localhost:9092
kafka topic producers orders --partition 0 --partition 1 --brokers localhost:9092
```

## txn

### txn abort

```sh
kafka txn abort
```

Write an abort marker for one in-flight transaction on a topic partition

| Flag                             | Description                                                    |
| -------------------------------- | -------------------------------------------------------------- |
| `--brokers <string>`             | comma-separated broker list, e.g. localhost:9092               |
| `--topic <string>`               | topic holding the in-flight transactional data                 |
| `--partition <number>`           | partition holding the in-flight transactional data             |
| `--producer-id <string>`         | the transaction's producer id                                  |
| `--producer-epoch <number>`      | the transaction's producer epoch                               |
| `--coordinator-epoch <number>`   | coordinator epoch, if known (resolved automatically otherwise) |
| `--transaction-version <number>` | transaction protocol version, if known                         |
| `--yes`                          | confirm the abort without an interactive prompt                |

Examples:

```sh
kafka txn abort --topic orders --partition 0 --producer-id 1000 --producer-epoch 0 --brokers localhost:9092 --yes
```

### txn describe

```sh
kafka txn describe <transactionalIds...>
```

Describe one or more transactional ids

| Flag                 | Description                                      |
| -------------------- | ------------------------------------------------ |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092 |

Examples:

```sh
kafka txn describe orders-producer-1 --brokers localhost:9092
```

### txn fence

```sh
kafka txn fence <transactionalIds...>
```

Fence out a transactional id's current producer, bumping its epoch

| Flag                 | Description                                                       |
| -------------------- | ----------------------------------------------------------------- |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092                  |
| `--timeout <number>` | transaction timeout in ms for the fenced producer (default 60000) |

Examples:

```sh
kafka txn fence orders-producer-1 --brokers localhost:9092
```

### txn list

```sh
kafka txn list
```

List transactions known to the cluster, optionally filtered

| Flag                                         | Description                                                 |
| -------------------------------------------- | ----------------------------------------------------------- |
| `--brokers <string>`                         | comma-separated broker list, e.g. localhost:9092            |
| `--state-filter <string> (repeatable)`       | a transaction state to filter on, e.g. Ongoing (repeatable) |
| `--producer-id-filter <string> (repeatable)` | a producer id to filter on (repeatable)                     |
| `--duration-filter <string>`                 | minimum transaction duration in ms                          |
| `--transactional-id-pattern <string>`        | a transactional id pattern to filter on                     |

Examples:

```sh
kafka txn list --brokers localhost:9092
kafka txn list --state-filter Ongoing --brokers localhost:9092
```

### txn terminate

```sh
kafka txn terminate <transactionalId>
```

Force-terminate a transactional id's current transaction

| Flag                 | Description                                           |
| -------------------- | ----------------------------------------------------- |
| `--brokers <string>` | comma-separated broker list, e.g. localhost:9092      |
| `--timeout <number>` | transaction timeout in ms for the fenced producer     |
| `--yes`              | confirm the termination without an interactive prompt |

Examples:

```sh
kafka txn terminate orders-producer-1 --brokers localhost:9092 --yes
```
