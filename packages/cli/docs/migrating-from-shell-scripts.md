# Migrating from the Kafka shell scripts

A map from the `kafka-*.sh` scripts shipped in an Apache Kafka distribution to the equivalent
`kafka` command, for anyone who already knows the shell tools and wants the shortest path to the
same operation here. It covers the **admin** surface only — `kafka-console-producer.sh` and
`kafka-console-consumer.sh` have no equivalent; this CLI does not produce or consume messages.

Two conventions hold everywhere below, so they aren't repeated per row:

- `--bootstrap-server host:port` becomes `--brokers host:port` (or is omitted entirely — see
  [Configuration](../README.md#configuration) for the other ways brokers resolve).
- `--command-config <properties file>` becomes either a `kafka.config.*` file (discovered
  automatically, or pointed at with `--config-file`) or a named `--profile`; there is no
  Java-properties-file equivalent.

Anything below marked **no direct equivalent** is still reachable through
[`kafka admin call <method>`](../README.md#commands) — the escape hatch that calls any `Admin`
method by name — as long as an underlying Admin API exists for it.

## `kafka-topics.sh`

| Shell invocation                                                                                                                                               | CLI equivalent                                                                                                                           |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `--list`                                                                                                                                                       | `kafka topic list`                                                                                                                       |
| `--describe --topic <name>`                                                                                                                                    | `kafka topic describe <name>`                                                                                                            |
| `--create --topic <name> --partitions N --replication-factor R`                                                                                                | `kafka topic create <name> --partitions N --replication-factor R`                                                                        |
| `--create --replica-assignment b:b,b:b`                                                                                                                        | `kafka topic create <name> --replica-assignment partition=replica,replica` (repeatable, one per partition)                               |
| `--create --config k=v`                                                                                                                                        | `kafka topic create <name> --config k=v` (repeatable)                                                                                    |
| `--create --if-not-exists`                                                                                                                                     | `kafka topic create <name> --if-not-exists`                                                                                              |
| `--alter --partitions N`                                                                                                                                       | `kafka topic add-partitions <name> --count N` (the new **total**, not a delta — same as the shell tool)                                  |
| `--alter --replica-assignment ...` (partition reassignment)                                                                                                    | `kafka cluster reassign execute` — see below; `kafka-topics.sh --alter` only ever changed partition count or assignment, never config    |
| `--alter --config ...`                                                                                                                                         | never valid on `kafka-topics.sh` either — use `kafka config set`                                                                         |
| `--delete --topic <name>`                                                                                                                                      | `kafka topic delete <name>` (prompts for confirmation; `--yes` skips it, matching `--if-exists` for tolerating an already-missing topic) |
| `--describe --under-replicated-partitions` / `--unavailable-partitions` / `--under-min-isr-partitions` / `--at-min-isr-partitions` / `--topics-with-overrides` | **no direct equivalent** — `kafka topic describe` always shows full partition detail; filter its `--json` output yourself                |
| `--exclude-internal`                                                                                                                                           | **no direct equivalent** — `kafka topic list`/`describe` always include internal topics                                                  |

## `kafka-configs.sh`

Entity types split across three command families here, matching how the config actually behaves
rather than one generic `--entity-type`:

| Shell entity type                                                                                                                 | CLI command                                                                                                                                                                                                                                               |
| --------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `topics`                                                                                                                          | `kafka config describe/set/unset --type topic`                                                                                                                                                                                                            |
| `brokers`                                                                                                                         | `kafka config describe/set/unset --type broker`                                                                                                                                                                                                           |
| `broker-loggers`                                                                                                                  | `kafka config describe/set/unset --type broker-logger`                                                                                                                                                                                                    |
| `client-metrics`                                                                                                                  | `kafka config describe/set/unset --type client-metrics`                                                                                                                                                                                                   |
| `groups`                                                                                                                          | `kafka config describe/set/unset --type group`                                                                                                                                                                                                            |
| `users` / `clients` (quota configs: `producer_byte_rate`, `consumer_byte_rate`, `request_percentage`, `controller_mutation_rate`) | `kafka quota describe`/`kafka quota alter --entity user=<name> --entity client-id=<id>` (`--entity` is repeatable, one per component, matching the shell tool's paired-entity support; `--entity user=` with no name targets that type's cluster default) |
| `users` (SCRAM credentials: `SCRAM-SHA-256`/`SCRAM-SHA-512`)                                                                      | `kafka scram list`/`kafka scram set`/`kafka scram delete`                                                                                                                                                                                                 |

| Shell invocation                                                                | CLI equivalent                                                                                                                    |
| ------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `--describe --entity-type topics --entity-name <name>`                          | `kafka config describe --type topic <name>`                                                                                       |
| `--alter --entity-type topics --entity-name <name> --add-config k=v`            | `kafka config set --type topic <name> --entry k=v`                                                                                |
| `--alter --entity-type topics --entity-name <name> --delete-config k`           | `kafka config unset --type topic <name> --key k`                                                                                  |
| `--describe --all`                                                              | `kafka config list-resources --type <type>`, then describe each — there is no single "every value including static defaults" call |
| `--topic <name>` (shorthand)                                                    | not a shorthand here — always spell out `--type topic <name>`                                                                     |
| `--broker-defaults` / `--client-defaults` / `--user-defaults` / `--ip-defaults` | **no direct equivalent** — a default-entity config isn't exposed yet                                                              |
| `--entity-type ips` (IP connection-rate quotas)                                 | **no direct equivalent** — `kafka quota` only models `user`/`client-id` entities                                                  |

## `kafka-acls.sh`

| Shell invocation                                                   | CLI equivalent                                                                                                                                                                                                                              |
| ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--list`                                                           | `kafka acl list` (every filter flag — `--topic`, `--group`, `--operation`, `--resource-pattern-type`, `--principal` — becomes the matching `kafka acl list --resource-name`/`--resource-type`/`--operation`/`--pattern-type`/`--principal`) |
| `--add --allow-principal P --allow-host H --operation O --topic T` | `kafka acl add P --resource-type topic --resource-name T --operation O --host H`                                                                                                                                                            |
| `--add --deny-principal ...`                                       | `kafka acl add P --permission-type deny ...`                                                                                                                                                                                                |
| `--add --producer --topic T`                                       | **no direct equivalent** — issue the three underlying ACLs (`write`, `describe`, `create`) as separate `kafka acl add` calls, or via `admin call createAcls --from-file`                                                                    |
| `--add --consumer --topic T --group G`                             | **no direct equivalent**, same reasoning — `read`+`describe` on the topic, `read` on the group                                                                                                                                              |
| `--remove <same filters as --list> --force`                        | `kafka acl remove <same filters> --yes`                                                                                                                                                                                                     |
| `--resource-pattern-type match` (fuzzy list filter)                | `kafka acl list --pattern-type match`                                                                                                                                                                                                       |
| `--idempotent`                                                     | **no direct equivalent** — grant `IdempotentWrite` explicitly with a second `kafka acl add ... --operation idempotent-write`                                                                                                                |

## `kafka-consumer-groups.sh`

| Shell invocation                                                                                 | CLI equivalent                                                                                                                                                                   |
| ------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--list`                                                                                         | `kafka group list` (`--state`/`--type` filters map to the same-named flags)                                                                                                      |
| `--describe --group G`                                                                           | `kafka group describe G` (`--members`/`--offsets`/`--state` become `kafka group describe G --members` etc.)                                                                      |
| `--delete --group G`                                                                             | `kafka group delete G` (`--yes` instead of no confirmation flag at all — the shell tool never asks)                                                                              |
| `--delete-offsets --group G --topic T`                                                           | `kafka group delete-offsets G --topic T`                                                                                                                                         |
| `--reset-offsets --group G --topic T --to-earliest`                                              | `kafka group reset-offsets G --topic T --to earliest`                                                                                                                            |
| `--reset-offsets ... --to-latest`                                                                | `kafka group reset-offsets G --topic T --to latest`                                                                                                                              |
| `--reset-offsets ... --execute`                                                                  | `kafka group reset-offsets G --topic T --to <target> --execute --yes` (without `--execute` both tools only preview)                                                              |
| `--reset-offsets --to-offset` / `--to-datetime` / `--by-duration` / `--shift-by` / `--from-file` | **no direct equivalent yet** — only the `earliest`/`latest` targets are modeled; the others need `admin call resetOffsets --from-file` with the target offsets computed yourself |
| (removing a member)                                                                              | `kafka group remove-members G --member <id>` — the shell tool has no equivalent at all; this is `LeaveGroup`-adjacent cleanup this CLI added                                     |
| `--validate-regex <pattern>`                                                                     | **no direct equivalent** — this is a client-side RE2 syntax check with no corresponding Admin API call                                                                           |

## `kafka-get-offsets.sh`

| Shell invocation                            | CLI equivalent                                                        |
| ------------------------------------------- | --------------------------------------------------------------------- |
| `--topic T --time latest`                   | `kafka topic offsets T` (defaults to the latest offset per partition) |
| `--topic T --time earliest`                 | `kafka topic offsets T --time earliest`                               |
| `--topic T --time <unix-ms>`                | `kafka topic offsets T --time <unix-ms>`                              |
| `--topic-partitions` (regex + range syntax) | **no direct equivalent** — pass one topic name at a time              |
| `--exclude-internal-topics`                 | not applicable — `kafka topic offsets` always takes an explicit topic |

## `kafka-delete-records.sh`

| Shell invocation                  | CLI equivalent                                                                                                                                         |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `--offset-json-file offsets.json` | `kafka topic delete-records --from-file offsets.json --yes` (same JSON shape: `{"partitions":[{"topic":"foo","partition":1,"offset":1}],"version":1}`) |

## `kafka-log-dirs.sh`

| Shell invocation                | CLI equivalent                                                                   |
| ------------------------------- | -------------------------------------------------------------------------------- |
| `--describe`                    | `kafka cluster log-dirs`                                                         |
| `--describe --topic-list t1,t2` | `kafka cluster log-dirs --topic t1 --topic t2` (repeatable, not comma-separated) |
| `--describe --broker-list 0,1`  | `kafka cluster log-dirs --broker 0 --broker 1` (repeatable, not comma-separated) |

## `kafka-reassign-partitions.sh`

| Shell invocation                                            | CLI equivalent                                                                                                                                                                                                              |
| ----------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--execute --reassignment-json-file plan.json`              | `kafka cluster reassign execute --from-file plan.json --yes` (same JSON shape)                                                                                                                                              |
| `--execute ... --throttle N`                                | **no direct equivalent** — throttling isn't wired up yet; set `leader.replication.throttled.rate`/`follower.replication.throttled.rate` yourself with `kafka config set --type broker`                                      |
| `--list`                                                    | `kafka cluster reassign list`                                                                                                                                                                                               |
| `--generate --topics-to-move-json-file f --broker-list 0,1` | **no direct equivalent** — this is a client-side planning step with no broker call behind it (it only reads topic/broker metadata to propose a plan); compute the plan yourself and feed it to `execute`                    |
| `--verify --reassignment-json-file plan.json`               | **no direct equivalent as a single command** — `kafka cluster reassign list` shows what's still in flight; once it's empty, the reassignment finished (this CLI does not yet auto-remove throttles the way `--verify` does) |
| `--cancel --reassignment-json-file plan.json`               | **no direct equivalent** — `admin call alterPartitionReassignments --from-file <plan with empty target replicas>`                                                                                                           |

Broker decommissioning (cordoning, then removal) is `kafka config set --type broker <id> --entry cordoned.log.dirs=*` followed by `kafka cluster unregister-broker --broker-id <id> --yes` — replacing both `kafka-configs.sh --add-config cordoned.log.dirs` and the separate `kafka-cluster.sh unregister` script.

## `kafka-leader-election.sh`

| Shell invocation                                    | CLI equivalent                                                                                                                                                              |
| --------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--election-type preferred --all-topic-partitions`  | `kafka cluster elect-leaders --election-type preferred --all-topic-partitions --yes`                                                                                        |
| `--election-type preferred --topic T --partition N` | `kafka cluster elect-leaders --election-type preferred --topic-partition T:N --yes`                                                                                         |
| `--election-type unclean ...`                       | `kafka cluster elect-leaders --election-type unclean ... --yes --force` (`--force` is this CLI's own extra gate on top of `--yes`, since an unclean election can lose data) |
| `--path-to-json-file f`                             | `kafka cluster elect-leaders --from-file f` (same JSON shape)                                                                                                               |

## Everything else

`kafka doctor`, `kafka init`, and `kafka profiles` have no shell-script equivalent at all — they
manage this CLI's own `kafka.config.*` file and connection diagnostics, a concept the shell tools
don't have (each one takes its own `--bootstrap-server`/`--command-config` on every invocation).
