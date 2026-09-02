# @cookiemonsterdev/kafka-cli

Command-line admin client for Apache Kafka, built on
[`@cookiemonsterdev/kafka-core`](../core/README.md). See the
[kafka monorepo](https://github.com/cookieMonsterDev/kafka).

## Install

```sh
npx @cookiemonsterdev/kafka-cli ping --brokers localhost:9092
# or
pnpm dlx @cookiemonsterdev/kafka-cli topic list --brokers localhost:9092
# or install it once
npm install -g @cookiemonsterdev/kafka-cli
kafka --version
```

Every command that connects takes `--brokers` directly (`localhost:9092`, or a comma-separated
list for a multi-broker cluster) — but it's optional. See [Configuration](#configuration) below
for the other ways brokers (and everything else) can be resolved.

## Commands

```sh
kafka init
kafka doctor
kafka profiles
kafka ping --brokers localhost:9092
kafka topic list --brokers localhost:9092
kafka topic describe orders --brokers localhost:9092
kafka topic create orders --brokers localhost:9092 --partitions 3 --replication-factor 1
kafka topic create orders payments --brokers localhost:9092   # fans out one call per topic
kafka topic delete orders --brokers localhost:9092 --yes
kafka topic add-partitions orders --count 6 --brokers localhost:9092
kafka topic offsets orders --brokers localhost:9092
kafka topic offsets orders --time earliest --brokers localhost:9092
kafka topic delete-records --from-file offsets.json --brokers localhost:9092 --yes
kafka topic producers orders --brokers localhost:9092
kafka config describe --type topic orders --brokers localhost:9092
kafka config set --type topic orders --entry retention.ms=604800000 --brokers localhost:9092
kafka config unset --type topic orders --key retention.ms --brokers localhost:9092
kafka config list-resources --type topic --brokers localhost:9092
kafka group list --brokers localhost:9092
kafka group describe my-group --brokers localhost:9092
kafka group offsets my-group --brokers localhost:9092
kafka group reset-offsets my-group --topic orders --to earliest --brokers localhost:9092
kafka group reset-offsets my-group --topic orders --to earliest --execute --yes --brokers localhost:9092
kafka group delete my-group --brokers localhost:9092 --yes
kafka group delete-offsets my-group --topic orders --brokers localhost:9092 --yes
kafka group remove-members my-group --member consumer-1-abc --brokers localhost:9092 --yes
kafka acl list --brokers localhost:9092
kafka acl add User:alice --resource-type topic --resource-name orders --operation read --brokers localhost:9092
kafka acl remove User:alice --resource-type topic --resource-name orders --brokers localhost:9092 --yes
kafka cluster info --brokers localhost:9092
kafka cluster reassign list --brokers localhost:9092
kafka admin methods
kafka admin call listTopics --brokers localhost:9092
kafka help topic create
kafka --version
```

`kafka topic create` accepts either `--partitions`/`--replication-factor` or an explicit
`--replica-assignment partition=replica,replica` (repeatable) — not both. `--config key=value`
(repeatable) sets topic-level configs. `--dry-run` validates without creating anything;
`--if-not-exists` treats an already-existing topic as success instead of a failure exit.

### Destructive topic operations

`topic delete` and `topic delete-records` refuse to run without confirmation: off a TTY (or under
`CI=true`), that means passing `--yes`; on a TTY, an interactive `[y/N]` prompt (asked on stderr)
does the same job. `cli.confirmDestructive: false` in the config file waives this prompt/`--yes`
requirement — but never the separate `--force` tier below, which no config setting can waive.

`topic delete` additionally requires `--force` before it will delete an internal (`__`-prefixed)
topic, or more than 10 topics in one call — both are treated as "you probably didn't mean to do
that in one shot" rather than as something to confirm away. `--if-exists` treats a topic that was
already gone as success instead of a failure exit. Deleting more than one topic fans out one
`deleteTopics` call per topic (core reports no per-topic result for a batched call), so a partial
failure exits `4` rather than masking which topics actually went away.

`topic add-partitions <topics...> --count <n>` raises each topic to `--count` total partitions —
not a delta, matching `kafka-topics.sh --alter --partitions`. `topic offsets <topic>` prints each
partition's current high/low watermark, or with `--time earliest|latest|max-timestamp|<ms>`, the
offset as of that point — the same three named timestamps `kafka-run-class
kafka.tools.GetOffsetShell` accepts, plus a literal millisecond epoch.

`topic delete-records --from-file <path>` deletes every record before the given offset, per
partition, reading the same JSON shape as `kafka-delete-records.sh --offset-json-file`:

```json
{
  "partitions": [{ "topic": "orders", "partition": 0, "offset": 3 }],
  "version": 1
}
```

A file spanning more than one topic fans out one `deleteTopicRecords` call per topic, again
exiting `4` on a partial failure. `topic producers <topic>` shows each queried partition's active
producer state (producer id, epoch, last sequence); with no `--partition` given it queries every
partition on the topic, and `--broker-id` targets one specific replica instead of the partition
leaders.

### Config commands

`--type` accepts a case-insensitive resource-type name (`topic`, `broker`, `broker-logger`,
`client-metrics`, `group`) or the broker's raw numeric code. `config describe <names...>` reads
back every config entry for one or more resources of that type — `--config-name` (repeatable)
narrows it to specific keys, and `--include-synonyms`/`--include-documentation` add the extra
detail those flags name. Describing more than one resource fans out one call per resource, so a
partial failure exits `4` rather than one bad name failing the whole batch. A config entry the
broker marks sensitive is redacted in the output unless `--show-secrets` is given.

`config set <names...> --entry key=value` (repeatable) and `config unset <names...> --key <key>`
(repeatable) apply an incremental set/delete to each named resource — `--dry-run` validates
without changing anything. Like `describe`, more than one resource name fans out one call per
resource. `config list-resources` lists every config resource the broker knows about, optionally
narrowed with a repeatable `--type`.

```sh
kafka config describe --type topic orders --include-synonyms --brokers localhost:9092
kafka config set --type topic orders --entry cleanup.policy=compact --brokers localhost:9092
kafka config unset --type topic orders --key cleanup.policy --brokers localhost:9092
kafka config list-resources --type topic --type group --brokers localhost:9092
```

### Group commands

`group list` prints every consumer group's id and protocol type. `group describe <groupIds...>`
reads back each group's state, join protocol, and member count — like `config describe`, the
broker's `DescribeGroups` response throws on the first requested group with a non-zero error
code and discards every other group's result in that same call, so describing more than one
group fans out one call per group id; a partial failure exits `4` rather than one bad group name
failing the whole batch. A group id the broker has never seen (or has fully forgotten) comes back
as `state: "Dead"` with no error code rather than an error, so that's treated as a failed describe
too, printing "group does not exist". `group offsets <groupId>` reads a group's committed offsets
— narrowed to specific topics with a repeatable `--topic`, or every topic the group has offsets for
by default —
without resolving or committing anything back to the broker.

`group reset-offsets <groupId> --topic <name> --to earliest|latest` is a dry run by default: it
prints the offset each topic's partitions _would_ move to, without changing anything and without
asking for confirmation. Passing `--execute` makes it real — gated behind the same `--yes`/
interactive-prompt tier as every other destructive command — and fans out one `resetOffsets` call
per `--topic`, exiting `4` on a partial failure.

`group delete <groupIds...>` and `group delete-offsets <groupId> --topic <name>` are both gated
behind confirmation. Deleting groups is a single call for the whole list (the broker's
`DeleteGroups` response either succeeds for every group or names only the ones that failed), so a
partial failure is read back from that per-group failure list rather than fanned out. Deleting a
group's offsets fans out one call per `--topic` (its response, like `DescribeGroups`, discards
every other topic's result once one partition in the call fails) — `--partition` (repeatable)
narrows which partitions are cleared for every named topic, or every partition of a topic is
discovered automatically when it's omitted. `group remove-members <groupId> --member <id>`
(repeatable, `memberId` or `memberId:groupInstanceId`) removes one or more static members from a
group's session in a single call; since the broker never fails that call outright, a member's own
result code decides whether it succeeded.

```sh
kafka group list --brokers localhost:9092
kafka group describe my-group --brokers localhost:9092
kafka group offsets my-group --topic orders --brokers localhost:9092
kafka group reset-offsets my-group --topic orders --to earliest --brokers localhost:9092
kafka group reset-offsets my-group --topic orders --to earliest --execute --yes --brokers localhost:9092
kafka group delete my-group --brokers localhost:9092 --yes
kafka group delete-offsets my-group --topic orders --brokers localhost:9092 --yes
kafka group remove-members my-group --member consumer-1-abc --brokers localhost:9092 --yes
```

### ACL commands

Every ACL flag — `--resource-type`, `--pattern-type`, `--operation`, `--permission-type` — accepts
a case-insensitive name (`topic`, `describe-configs`, `allow`, …) or the broker's raw numeric code;
an unrecognized value is a usage error listing every valid choice. `acl list` filters on resource
type, resource name, pattern type, principal, host, operation, and permission type — any field left
unset defaults to `any`, matching `describeAcls`'s own filter semantics — and renders one row per
resource/principal pair.

`acl add <principals...> --resource-type <type> --resource-name <name> --operation <op>` creates an
ACL entry for every principal x `--operation` combination (both repeatable), so granting several
operations to several principals in one invocation reports each combination's own success or
failure rather than one combined result. `--pattern-type` defaults to `literal`, `--permission-type`
to `allow`, and `--host` to `*`. The underlying `createAcls` call has no `validateOnly` of its own,
so `--dry-run` prints the entries that would be created and exits without ever opening an admin
connection.

`acl remove <principals...>` deletes every ACL matching a filter, gated behind confirmation like
`group delete`. Filters default to `any` exactly like `acl list`, and the command fans out one
`deleteAcls` call per principal, exiting `4` on a partial failure; a successful principal reports
how many ACLs actually matched and were removed.

```sh
kafka acl list --brokers localhost:9092
kafka acl list --resource-type topic --resource-name orders --brokers localhost:9092
kafka acl add User:alice --resource-type topic --resource-name orders --operation read --brokers localhost:9092
kafka acl add User:alice User:bob --resource-type topic --resource-name orders --operation read --operation write --brokers localhost:9092
kafka acl remove User:alice --resource-type topic --resource-name orders --brokers localhost:9092 --yes
```

### Cluster commands

`cluster info` describes the cluster's brokers, controller, and cluster id. `cluster quorum`
describes the metadata quorum — each `__cluster_metadata` partition's leader, voters, and
observers. `cluster features` lists supported vs. finalized feature versions, merged by name.
`cluster log-dirs` describes every broker's log directories and their partition sizes, narrowed
with a repeatable `--broker`/`--topic` — the topic filter is applied client-side, matching
`kafka-log-dirs.sh --topic-list` itself rather than a server-side option.

`cluster update-features --feature name=maxVersionLevel` (repeatable) upgrades, safe-downgrades,
or unsafe-downgrades one or more finalized features in one call — `--upgrade-type` applies to
every named feature, and `--dry-run` maps to the API's own `validateOnly`. An unsafe downgrade
additionally requires `--force`, since it can lose data. A partial per-feature failure is reported
per feature rather than failing the whole call.

`cluster elect-leaders --election-type preferred|unclean` triggers a leader election over exactly
one of `--topic-partition topic:partition` (repeatable), `--all-topic-partitions`, or `--from-file`
(the `kafka-leader-election.sh --path-to-json-file` shape). `--election-type unclean` requires
`--force`, since it can lose data; `--dry-run` prints the election target without connecting.

`cluster reassign list` lists every active partition reassignment — like the real
`kafka-reassign-partitions.sh --list`, it takes no topic filter. `cluster reassign execute
--from-file <path>` submits a reassignment from the `kafka-reassign-partitions.sh
--reassignment-json-file` shape, gated behind confirmation; `--dry-run` prints the plan without
connecting. That file's `log_dirs` entries are parsed and validated but not applied — moving a
replica between log dirs on the same broker needs a separate command.

`cluster unregister-broker --broker-id <id>` decommissions a broker from a KRaft cluster, gated
behind confirmation. `cluster raft-voter add --voter-id <id> --voter-directory-id <uuid> --listener
name=host:port` (repeatable) and `cluster raft-voter remove --voter-id <id> --voter-directory-id
<uuid>` add or remove a voter from the metadata quorum — `remove` is gated behind confirmation like
every other removal in this package.

```sh
kafka cluster info --brokers localhost:9092
kafka cluster quorum --brokers localhost:9092
kafka cluster features --brokers localhost:9092
kafka cluster log-dirs --topic orders --brokers localhost:9092
kafka cluster update-features --feature kraft.version=1 --brokers localhost:9092 --yes
kafka cluster elect-leaders --election-type preferred --all-topic-partitions --brokers localhost:9092
kafka cluster reassign list --brokers localhost:9092
kafka cluster reassign execute --from-file reassignment.json --brokers localhost:9092 --yes
kafka cluster unregister-broker --broker-id 3 --brokers localhost:9092 --yes
kafka cluster raft-voter add --voter-id 4 --voter-directory-id 3c48b6f0-1234-4a5b-8c9d-0123456789ab --listener CONTROLLER=localhost:9093 --brokers localhost:9092
kafka cluster raft-voter remove --voter-id 4 --voter-directory-id 3c48b6f0-1234-4a5b-8c9d-0123456789ab --brokers localhost:9092 --yes
```

`kafka admin call <method>` reaches every method on `Admin`, including ones without a first-class
command yet — `kafka admin methods` lists all of them and which ones are mounted vs.
passthrough-only. Arguments come from a JSON file (`--from-file`); a string value prefixed
`bigint:`, `base64:`, or `uuid:` decodes to a real `bigint`/`Buffer` before the call. A method that
isn't read-only refuses to run without both `--yes` and `--force`.

## Configuration

`kafka init` scaffolds a `kafka.config.ts` (or `.mjs`, with `--js`) file in the current directory —
`.ts` when a `tsconfig.json` or a `typescript` dependency is detected, `.mjs` otherwise:

```sh
kafka init
kafka init --force   # overwrite an existing file
```

```ts
export default {
  client: {
    brokers: ['localhost:9092'],
  },
};
```

Every connecting command resolves its options from, highest precedence first: **the command's own
flags → environment variables → the active `--profile` → the config file's `client` section →
built-in defaults**. The config file is discovered by walking upward from the current directory
looking for `kafka.config.{ts,mts,cts,js,mjs,cjs,json}` (or `.config/kafka.*`), stopping at the
first `.git`, `pnpm-workspace.yaml`, or workspace `package.json` — override that with
`--config-file <path>` or `KAFKA_CONFIG`.

Named alternates — e.g. one entry per cluster — go under a `cli.profiles` section and are selected
with `--profile <name>` or `KAFKA_PROFILE`:

```ts
export default {
  cli: {
    profiles: {
      staging: { brokers: ['staging-1:9092', 'staging-2:9092'] },
      production: {
        brokers: ['prod-1:9092', 'prod-2:9092'],
        sasl: { mechanism: 'plain', username: '...', password: '...' },
      },
    },
  },
};
```

```sh
kafka --profile staging topic list
kafka profiles          # lists every configured profile, marking the active one
kafka doctor            # reports the config file path, active profile, and where every value came from
```

The `cli:` section also accepts `output` (`"human"`/`"json"`, the lowest-precedence source for
`--json`/`--format`/`KAFKA_OUTPUT`), `timeoutMs` (a last-resort default for the connection and
request timeouts), and `topicDefaults` (`partitions`/`replicationFactor` applied to `topic create`
whenever the equivalent flag is omitted — never alongside an explicit `--replica-assignment`). An
unknown key inside `cli:` warns on stderr; it's never a hard error, so a config written for a newer
CLI still loads.

`confirmDestructive: false` skips the `--yes`/interactive-prompt tier in front of a destructive
command (`topic delete`, `topic delete-records`, `group delete`, `group delete-offsets`,
`group remove-members`, `group reset-offsets --execute`, and `acl remove`) — see [Destructive topic
operations](#destructive-topic-operations), [Group commands](#group-commands), and
[ACL commands](#acl-commands). It never waives a command's own `--force`-gated safety checks.

## Output

Human output goes to stdout; `--json` (or `--format json`, or `KAFKA_OUTPUT=json`) puts exactly
one JSON document on stdout instead — a `bigint` becomes a decimal string and a `Buffer` becomes
base64 (a 16-byte `topicId` becomes its UUID). `-q`/`--quiet` silences everything but errors;
`-v`/`--verbose` (repeatable) adds detail, all of it on stderr. `--no-color`/`NO_COLOR` disable
color; `FORCE_COLOR` forces it even off a TTY.

## Exit codes

| Code  | Meaning                             |
| ----- | ----------------------------------- |
| `0`   | ok                                  |
| `1`   | operation failed                    |
| `2`   | usage error                         |
| `3`   | config error                        |
| `4`   | partial batch failure               |
| `5`   | aborted / unconfirmed               |
| `6`   | unsupported by the connected broker |
| `7`   | authentication failed               |
| `70`  | internal bug — please report it     |
| `130` | interrupted (`SIGINT`)              |
| `143` | terminated (`SIGTERM`)              |

## Development

From the repo root:

```sh
pnpm --filter @cookiemonsterdev/kafka-cli build
pnpm --filter @cookiemonsterdev/kafka-cli test
pnpm --filter @cookiemonsterdev/kafka-cli typecheck
KAFKA_VERSION=4.0 pnpm --filter @cookiemonsterdev/kafka-cli test:integration
```

See the root [CONTRIBUTING.md](../../CONTRIBUTING.md) for the full workspace workflow.
