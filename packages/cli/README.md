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

`confirmDestructive` is accepted and validated today but has no effect yet — it's reserved for a
future confirmation layer in front of destructive operations (none of which this CLI has yet).

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
