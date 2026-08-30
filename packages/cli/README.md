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

There's no config-file support yet — every command that connects takes `--brokers` directly
(`localhost:9092`, or a comma-separated list for a multi-broker cluster).

## Commands

```sh
kafka ping --brokers localhost:9092
kafka topic list --brokers localhost:9092
kafka topic describe orders --brokers localhost:9092
kafka topic create orders --brokers localhost:9092 --partitions 3 --replication-factor 1
kafka topic create orders payments --brokers localhost:9092   # fans out one call per topic
kafka admin methods
kafka admin call listTopics --brokers localhost:9092
kafka help topic create
kafka --version
```

`kafka topic create` accepts either `--partitions`/`--replication-factor` or an explicit
`--replica-assignment partition=replica,replica` (repeatable) — not both. `--config key=value`
(repeatable) sets topic-level configs. `--dry-run` validates without creating anything;
`--if-not-exists` treats an already-existing topic as success instead of a failure exit.

`kafka admin call <method>` reaches every method on `Admin`, including ones without a first-class
command yet — `kafka admin methods` lists all of them and which ones are mounted vs.
passthrough-only. Arguments come from a JSON file (`--from-file`); a string value prefixed
`bigint:`, `base64:`, or `uuid:` decodes to a real `bigint`/`Buffer` before the call. A method that
isn't read-only refuses to run without both `--yes` and `--force`.

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
