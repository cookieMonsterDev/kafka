---
name: kafka-cli
description: >
  Use when administering an Apache Kafka cluster from a terminal, shell script, or CI job with the
  `kafka` command from @cookiemonsterdev/kafka-cli: checking connectivity, listing, creating,
  describing, or deleting topics, inspecting consumer or share groups and lag, resetting group
  offsets, changing configs, managing ACLs, SCRAM users, quotas, delegation tokens, transactions,
  or KRaft quorum, setting up kafka.config profiles, or parsing --json output and exit codes.
metadata:
  purpose: >-
    Runs cluster operations through the kafka CLI with the right connection, safe confirmation
    flags for destructive commands, and machine-readable output for scripts.
  type: core
  library: '@cookiemonsterdev/kafka-cli'
  library_version: '1.10.1'
sources:
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/cli/start/installation.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/cli/start/getting-started.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/cli/guides/configuration.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/cli/guides/output-and-scripting.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/cli/reference/commands.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/cli/reference/exit-codes.md'
---

# kafka-cli

A `kafka` command-line admin client built on `@cookiemonsterdev/kafka-core`. It covers cluster
administration. It does not produce or consume messages: for that, write code with the
`kafka-core` skills or open the studio (`kafka studio`, see the `kafka-studio` skill).

## Setup

Requires Node.js 24+ or Bun 1.4+.

```sh
npx @cookiemonsterdev/kafka-cli ping --brokers localhost:9092   # one-off, no install
npm install --save-dev @cookiemonsterdev/kafka-cli              # per project
npm install -g @cookiemonsterdev/kafka-cli                      # global `kafka` binary
```

Use the project-local binary (`npx kafka …`) when the CLI is a dev dependency.

## Core Patterns

### Connect once with a config file

```sh
kafka init      # writes ./kafka.config.ts with client.brokers = ['localhost:9092']
kafka doctor    # shows which config file, profile, and values were resolved, and tests the connection
```

Resolution order, highest first: command flags → environment variables (`KAFKA_BROKERS`,
`KAFKA_SASL_*`, `KAFKA_SSL*`, …) → `--profile` / `KAFKA_PROFILE` → the config file's `client`
section → defaults. The file is discovered by walking up from the current directory (stopping at
`.git`, `pnpm-workspace.yaml`, or a workspace `package.json`); override it with
`--config-file <path>` or `KAFKA_CONFIG`.

Named clusters go under `cli.profiles`:

```ts
// kafka.config.ts
export default {
  client: { brokers: ['localhost:9092'] },
  cli: {
    profiles: {
      staging: { brokers: ['staging-1:9092', 'staging-2:9092'] },
      production: {
        brokers: ['prod-1:9093'],
        ssl: true,
        sasl: { mechanism: 'scram-sha-512', username: 'ops', password: process.env.KAFKA_PASSWORD ?? '' },
      },
    },
  },
};
```

```sh
kafka --profile staging topic list
kafka profiles
```

### Everyday commands

```sh
kafka ping
kafka topic list
kafka topic create orders --partitions 3 --replication-factor 1
kafka topic describe orders
kafka topic offsets orders
kafka group list
kafka group describe my-group
kafka group offsets my-group
kafka config describe --help
```

Command families: `topic`, `group`, `share-group`, `config`, `acl`, `scram`, `quota`, `token`,
`txn`, `cluster`, plus `ping`, `doctor`, `init`, `profiles`, `completion`, `studio`. Run
`kafka <command> --help` for exact flags before using a command for the first time. For any Admin
method without its own command, use `kafka admin methods` and `kafka admin call <method>`.

### Destructive commands

Commands that delete or reset data prompt for confirmation. In a script (stdin not a TTY) they
exit `5` unless `--yes` is passed. Some need more:

```sh
# Preview, then apply, an offset reset (stop the group's consumers first)
kafka group reset-offsets my-group --topic orders --to earliest
kafka group reset-offsets my-group --topic orders --to earliest --execute --yes

kafka topic delete orders --yes
kafka admin call createAcls --from-file ./acls.json --yes --force
```

### Scripting with `--json`

`--json` (or `KAFKA_OUTPUT=json`) writes exactly one JSON document to stdout. Offsets and other
`bigint` values are decimal strings; `Buffer` values are base64 (a `topicId` is a UUID string).
Logs from `-v`/`-q` go to stderr.

```sh
kafka topic create orders payments --json > result.json
code=$?
if [ "$code" -ne 0 ] && [ "$code" -ne 4 ]; then exit "$code"; fi
jq '.results[] | select(.ok == false)' result.json
```

Exit codes: `0` ok, `1` failed, `2` usage, `3` config error, `4` partial batch failure,
`5` aborted/unconfirmed, `6` unsupported by the broker, `7` authentication failed.

## Common Mistakes

- **CRITICAL — Running a destructive command against the wrong cluster.** A config file from a
  parent directory or a leftover `KAFKA_PROFILE` can point elsewhere. Run `kafka doctor` (or
  pass `--brokers`/`--profile` explicitly) before `delete`, `reset-offsets`, `txn abort`, or
  `cluster` changes.
- **HIGH — Adding `--yes` to skip a prompt without reviewing the preview.** For
  `group reset-offsets`, run once without `--execute` and read the result first.
- **HIGH — Parsing offsets from `--json` as JSON numbers.** They are strings on purpose; convert
  with `tonumber` only when values stay below 2^53.
- **MEDIUM — Treating exit code `4` as total failure (or as success).** Some items succeeded;
  inspect each item's `ok` in the JSON body.
- **MEDIUM — Committing credentials in `kafka.config.ts` profiles.** Read them from
  `process.env` as in the example above.

## Completion

- `kafka doctor` reports the intended config file, profile, and a working connection.
- A follow-up read command (`topic describe`, `group offsets`, `acl list`, …) shows the intended
  state.
- Scripts check the exit code before parsing stdout.

## References

- Command reference (one page per command):
  https://cookiemonsterdev.github.io/kafka/docs/cli/reference/commands/
