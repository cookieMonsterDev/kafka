# kafka-core comparative benchmarks

Compares `@cookiemonsterdev/kafka-core`, built from `origin/develop`, with other Kafka clients on several
runtimes. Every measured process runs in its own Docker container against a single-node broker, under a fixed
CPU and memory budget.

| Client                           | Kind                                         |
| -------------------------------- | -------------------------------------------- |
| `@cookiemonsterdev/kafka-core`   | Pure JS, wire protocol; built from a git SHA |
| `@platformatic/kafka`            | Pure JS, wire protocol                       |
| `kafkajs`                        | Pure JS                                      |
| `@confluentinc/kafka-javascript` | Native addon over librdkafka (KafkaJS API)   |

Competitor versions are pinned exactly in [`package.json`](package.json); every result records the versions it
ran with.

Runtimes: Node.js 24, Node.js 26, and the latest Bun 1.x. All three images are Debian 13 (trixie), so every
cell has the same glibc and allocator.

This is a standalone project, not a member of the repository's pnpm workspace. Its dependencies are installed
only inside the Docker images; the runner on the host uses nothing but Node.js built-ins. It does not replace
`packages/core/bench/`, which measures kafka-core internals without a broker.

Results are valid as ratios on the machine that produced them, not as absolute numbers for other hardware.

## Requirements

- Docker with at least 6 CPUs and 8 GiB for its VM (Docker Desktop: **Settings → Resources**).
- Node.js 24 or newer on the host, and `git`. No `pnpm install` is needed to run benchmarks.
- On macOS, for a run that counts: AC power, Low Power Mode off, no thermal throttling, and no other containers
  running. `doctor` checks all of these.
- Nothing else heavy running. The runner takes a machine-wide lock (`/tmp/kafka-serial.lock`, a directory holding
  the owner's pid) so two sessions never overlap. Another tool that takes the same lock around its own
  commands can start the runner as a child process; the runner then treats the lock as already held.

## Running

From the repository root:

```sh
pnpm benchmarks:doctor                         # host and Docker checks, resource budget
pnpm benchmarks:run --tier smoke               # harness check in minutes; never publishable
pnpm benchmarks:run --tier standard            # the headline comparison, 1 KiB values with headers
pnpm benchmarks:run --tier standard --payload small   # the 100 B per-record overhead stress test
pnpm benchmarks:run --tier full                # every scenario; expect hours
pnpm benchmarks:report                         # re-aggregate the latest session
pnpm benchmarks:publish                        # copy a session to results/published/ if it passes the guards
```

Or from `benchmarks/`: `node runner/cli.ts <command>`. `node runner/cli.ts --help` lists every flag, for example
`--clients`, `--runtimes`, `--scenarios`, `--repetitions`, `--seed`, `--count-scale`, `--aa` and `--core-ref`.

`--core-ref <ref>` builds kafka-core from another ref, and `--core-ref WORKTREE` from the current checkout,
including uncommitted changes. Both are useful for before/after checks of performance work; only a SHA on
`origin/develop` can be published.

Tiers: `smoke` runs P1, P3 and C1 once with small counts and the `normalized` profile only, to check the
harness. `standard` and `full` run 5 repetitions under both profiles; which scenarios belong to which tier is set
in [`client/spec.ts`](client/spec.ts), and the message counts per tier and payload in
[`runner/schedule.ts`](runner/schedule.ts).

### Calibrating before the first published run

Each payload has its own `standard` counts. P2 is calibrated for both, from P2-only sessions on 2026-11-16. With
1,000,000 records of 1 KiB (40,000 warm-up), the slowest series, KafkaJS on Node.js 24, measured 9.3 s, so the
default is 1,200,000 records with 60,000 warm-up. With 3,000,000 records of 100 B (120,000 warm-up), the slowest,
Confluent, measured 15.2 s; the default keeps 3,000,000 records with 100,000 warm-up.

For 1 KiB, session `20261116193938-e9d9dad43d38` put the slowest P1 and P3 medians at 0.80 s for 7,500 records
and 12.1 s for 250,000 records. Those rates project to about 13 s and 17 s at the new counts. P4 and P5 use
initial estimates based on P3. C1–C4 use 1,350,000 measured records and 50,000 warm-up records, near the
test's storage budget. At the observed C1 and C2 rates, the fastest cells are projected to take about 0.8 s.
Verify the new counts in Docker before publishing, especially Confluent's consume runs under the 2 GiB client cap.

Before publishing on a machine:

1. Select a scenario with `--scenarios` and check its estimated topic size, including warm-up, before raising
   `--count-scale`. Aim for 10–20 s for the slowest client or stop when the topic reaches the storage budget.
   Update `runner/schedule.ts` with the resulting counts. The runner deletes each topic and waits for its files
   to go before starting the next run. `runner/schedule.test.ts` checks that every default topic fits within
   75% of the broker's 2 GiB tmpfs. This leaves room for internal topics and for error in the size estimate.
2. Run an A/A session: `pnpm benchmarks:run --tier standard --payload medium --clients kafka-core --aa`.
   Match the payload and tier of the comparison you plan to publish. This adds a second
   kafka-core series, `kafka-core#2`; the report lists the largest difference between the two per scenario.
   That is the noise floor of the machine. A/A sessions themselves cannot be published.
3. Pass the largest A/A floor among the scenarios and metrics you plan to compare to `report` and `publish`:
   `--noise-floor 0.03`. Differences inside the floor are reported as inconclusive. Set `--noise-threshold`
   separately if you need a coefficient-of-variation threshold other than 5%.

### What one session does

1. `doctor`: takes the lock, checks the host, derives the resource budget, writes `environment.json`.
2. Resolves `origin/develop` (after `git fetch origin develop`; the working tree and current branch are not
   touched) and builds one image per runtime. kafka-core and kafka-config are built and packed from that SHA, so
   develop's core is tested with develop's config.
3. Starts the broker and probes every (client, runtime) cell: import, connect, produce one record, consume it.
   Unsupported cells are recorded with a reason in `support-matrix.json` and skipped. Probe results are cached
   per image.
4. Warms up the broker's JVM with throwaway runs.
5. For each scheduled run: creates a fresh topic, seeds it for consume scenarios, starts a fresh client
   container, verifies the result against the broker, deletes the topic and waits for the broker to go idle.
6. Aggregates, writes `summary.json` and `report.md`, and stops the broker.

## Resource budget

Derived from `docker info` at run time ([`runner/budget.ts`](runner/budget.ts)). With 6 VM CPUs:

| Container         | vCPUs (cpuset) | Memory                             | Notes                                 |
| ----------------- | -------------- | ---------------------------------- | ------------------------------------- |
| Kafka broker      | 3 (`0-2`)      | 5 GiB: 2 GiB heap, 2 GiB tmpfs log | Stays up for the session              |
| Client under test | 2 (`3-4`)      | 2 GiB, swap off                    | One fresh container per run           |
| Unassigned        | 1 (`5`)        | rest                               | VM kernel, dockerd, container network |

With 8 or more VM CPUs the broker gets `0-3` and the client `4-5`. Two client CPUs leave room for work off the
main thread (librdkafka threads, worker threads, GC helpers); every result also reports CPU seconds, so "faster
because it used more CPU" stays visible.

On macOS the VM's vCPUs cannot be pinned to performance cores. That is the main remaining source of noise, and the
reason for interleaved repetitions and the A/A test.

The broker is `apache/kafka:4.4.0` in KRaft mode, PLAINTEXT only, on an internal Docker network with no published
ports ([`docker/compose.yml`](docker/compose.yml)). Client containers have no internet access, no bind mounts, a
read-only root filesystem, all capabilities dropped, `no-new-privileges` and a pids limit.

## Scenarios and metrics

The scenario catalog, including the wire settings of each scenario, is [`client/spec.ts`](client/spec.ts).
Payloads come from a fixed seed. `medium`, the default and headline, is a 1 KiB value with a 16 B key and 2
headers with 16 B values. `small` is a 100 B value with a 16 B key and no headers: a stress test of per-record
client overhead, where differences between clients come mostly from the work each does per record. Topics have
3 partitions.

| ID  | Name                   | Measures                                                                          |
| --- | ---------------------- | --------------------------------------------------------------------------------- |
| P1  | `produce-single`       | Sequential `await send()` of one record, acks 1, no linger                        |
| P2  | `produce-batch`        | Sequential `await send()` of 100 records; the headline produce comparison         |
| P3  | `produce-throughput`   | Up to 256 concurrent single-record `send()` calls, linger 5 ms, 16 KiB batches¹   |
| P4  | `produce-gzip`         | P3 with gzip                                                                      |
| P5  | `produce-idempotent`   | P3 with idempotence and acks all                                                  |
| C1  | `consume-each-message` | Read a preloaded topic from the start with a per-record handler                   |
| C2  | `consume-each-batch`   | The same through the batch API                                                    |
| C3  | `consume-gzip`         | C1 on a gzip-seeded topic                                                         |
| C4  | `consume-commit`       | C1 with an awaited offset commit every 1000 records                               |
| E1  | `e2e-latency`          | Open-loop fixed-rate producer and a consumer; latency from the intended send time |
| L1  | `startup`              | Process start to the first acknowledged produce and to the first consumed record  |
| F1  | `footprint`            | Install size, dependencies and files, required and optional; information only²    |

¹ P3, and P4 and P5 built on it, are structurally biased. Each of the 256 workers awaits its own single-record
`send()`, so the result shows whether and when a client merges records from separate calls, not protocol speed.
kafkajs and Platformatic send one request per call, librdkafka waits out `linger.ms` for each partition batch, and
kafka-core flushes once its buffered bytes across all partitions reach `batchSize`
([option mapping](client/adapters/option-mapping.md#kafka-core)). Use P2 as the headline produce comparison; the
report repeats this note under each of these scenarios.

² F1 is read from the image, not run. For each client it reports bytes, files and dependencies twice: the required
part is the package and everything it reaches through `dependencies`; the optional part is what installed
`optionalDependencies` add on top. It names no smaller or larger client, because a client's size follows what it
implements and bundles. A session recorded before this split measured only totals, without file counts; once
re-aggregated, its rows show those totals and mark the required and optional cells as not split.

Each run warms up in-process, then measures. Per run: records/s and MB/s (10^6 bytes, values only), latency
percentiles from a preallocated sample buffer, process and cgroup CPU seconds, records per CPU second, cgroup peak
memory, heap, GC count and time, and event-loop delay p99 (the last three are `null` where the runtime lacks the
API).

### Comparison rule

Repetitions run in fresh containers and are interleaved: each round runs every cell once, in an order shuffled
with the session's recorded `--seed`, so drift spreads across clients. The record payloads use a separate, fixed
seed, so every session sends the same bytes. The report shows the median with min–max and the coefficient of
variation (CV). A cell whose CV is above the noise threshold (5%, or the `--noise-threshold` passed in) is marked
noisy.

A comparison of two cells is `inconclusive`, and names neither as better, when any of these holds:

1. Either cell is noisy. This check comes first, so two noisy cells stay inconclusive even when their min–max
   ranges do not overlap.
2. Their min–max ranges overlap.
3. Their medians differ by no more than the noise floor: the `--noise-floor` passed in, or the A/A floor measured
   in the same session, whichever is larger.

Otherwise the cell with the better median is better. Throughput scenarios compare records/s, L1 compares time to
the first acknowledged produce, and E1 compares p99 and p50 latency, each with its own CV and A/A floor. Every
chart on the docs page, peak memory included, names a highest or lowest client on a runtime only when this rule
puts it ahead of every other client there, and otherwise says there is no clear one.

## Fairness rules

1. Scenarios are specified in wire-level terms (acks, linger, batch bytes, compression, in-flight requests,
   idempotence, fetch sizes and wait). Each adapter maps them to its library's options.
2. Two profiles: `normalized` applies the same wire settings to every client and is the headline; `defaults` sets
   only brokers, client id and group id, showing what a user gets out of the box. `defaults` runs for P3 and C1.
3. Gaps are declared, not hidden: when a library cannot express a setting, its adapter reports it and the report
   footnotes it. Differences outside the wire settings are declared the same way. For example, only kafka-core
   verifies record-batch CRCs on consume by default, so the other clients' consume and E1 results carry a CRC
   footnote.
4. Adapters are thin and symmetric: the same payload bytes, `Buffer` keys and values, no serializers, logging off,
   the same call shape, and the same commit policy.
5. Every run is verified: broker end offsets must equal what was produced; consumed count and bytes must equal
   what was seeded. A mismatch fails the run.
6. Consume topics are seeded by Kafka's own `kafka-producer-perf-test.sh` inside the broker container, so no
   contender writes the data another one reads. The seeder writes values only, without keys or headers.
7. Versions, date and environment travel with the numbers. Competitor versions are pinned exactly; a bump means a
   re-run.

The option mapping for each competitor, with the gaps and the upstream benchmark code it was checked against, is in
[`client/adapters/option-mapping.md`](client/adapters/option-mapping.md).

## Support findings

Recorded by the probe on 2026-11-12, with the pinned versions:

- kafka-core, `@platformatic/kafka` and `kafkajs` run on all three runtimes.
- `@confluentinc/kafka-javascript` runs on Node 24 only. There is no prebuilt binary for Node 26 (ABI 147), and
  its source build fails while linking: GNU `ar` cuts MRI-script paths at `@`, and every install path contains
  `node_modules/@confluentinc/`. Bun 1.4 reports ABI 147 as well, so the Node 24 binary is rejected there too.
  The package is an optional dependency, so these cells are recorded as unsupported instead of failing the build.

### Known issues

Recorded from session `20261112113347-fe9c38b14aa8` (tier `standard`, 2026-11-12):

- **Confluent runs out of memory while consuming.** 4 of 450 runs failed with exit code 137 and no stderr: Confluent
  on Node 24, C1 `normalized` repetition 3 and C2 repetitions 3 to 5. Passing Confluent consume runs peaked at
  979–2,029 MiB against the 2 GiB client limit, so these are out-of-memory kills inside the library's process, not
  kafka-core failures. The limit stays the same for every client to keep results comparable. `publish` refuses a
  session with these failures.
- **100 B consume runs finish quickly for faster clients.** With 2,000,000 measured records, the slowest
  passing series took 11.3 s for C1 `normalized` and 8.2 s for C2 (both Confluent; only 2 of its 5 C2 runs passed).
  kafka-core's C1 `defaults` took about 50 s on Node because it committed every 500 records; that build predates
  the once-per-batch default. Every other series finished in 0.9–6.4 s, which makes those cells the noisiest.
  The 100 B counts stay unchanged because larger topics would push Confluent into more out-of-memory failures.

## Results

- `results/raw/<UTC yyyymmddHHMMSS>-<core sha>/`: one directory per session, gitignored. It holds
  `session.json`, `environment.json`, `support-matrix.json`, `footprint.json`, `runs/<runId>.json`,
  `summary.json` and `report.md`.
- `results/published/`: committed summaries, one per published session, plus `latest.json`. The docs site reads
  these, so it never runs benchmarks itself.
- [`results/schema/summary.schema.json`](results/schema/summary.schema.json): the schema every summary must match.
  Bump `schemaVersion` for incompatible changes.

`publish` refuses a session with doctor warnings (including a forced run), a kafka-core build that is not a SHA
on `origin/develop`, a `smoke` tier, an A/A series, an unfinished session, any failed or unverified run, or fewer
repetitions than `--min-repetitions` (default 5). Publishing is always a deliberate command.

### Docs page

The [Benchmarks page](../packages/docs/src/content/docs/core/reference/benchmarks.md) is generated by
[`scripts/generate-benchmark-docs.mjs`](../scripts/generate-benchmark-docs.mjs) from one session per payload:

```sh
pnpm docs:benchmarks --competitors benchmarks/results/raw/<session> --core benchmarks/results/raw/<session>
pnpm docs:benchmarks           # re-render from the committed packages/docs/src/data/benchmarks/comparison.json
pnpm docs:benchmarks --check   # fail if the page or the JSON is out of date
```

A session replaces only the results of its own payload, so the 1 KiB and 100 B sessions can be run separately.
The page prints the setup each session ran: payload shape and an example record, record counts, warm-up and
measured time, the pinned wire settings per scenario, the broker, the host and the resource limits. A chart
whose headline payload has no published results shows the other payload, and its subtitle names the payload.

## Development

```text
runner/            host-side orchestrator (Node.js built-ins only)
  cli.ts           doctor | probe | run | report | publish | typecheck
  doctor.ts budget.ts lock.ts              host checks, resource budget, serial lock
  core-source.ts images.ts probe.ts footprint.ts   kafka-core build, images, support matrix, F1
  broker.ts container.ts attempt.ts                broker, client containers, attempt resource ownership
  schedule.ts session.ts                          matrix, session files
  aggregate.ts report.ts publish.ts schema.ts      statistics, Markdown, publishing guards
client/            runs inside the container on the runtime under test
  main.ts          reads BENCH_SPEC, loads one adapter, prints one result line
  measure.ts payload.ts spec.ts
  adapters/        one file per client, plus shared.ts and the option mapping
  scenarios/       one file per scenario ID
docker/            Dockerfile (core-builder, deps, runtime stages) and the broker's compose.yml
```

- Code runs as TypeScript without a build step (Node.js type stripping, Bun natively). Relative imports therefore
  use explicit `.ts` extensions, unlike the extensionless imports in `packages/*`.
- `node runner/cli.ts typecheck` type-checks `client/` and `runner/` inside the image against the installed
  clients. For editor support and a quick host check, run `pnpm install --ignore-scripts` in this directory and
  `pnpm typecheck` (the Confluent addon is not built on the host).
- `pnpm test` (in this directory) runs results-pipeline, attempt-lifecycle and F1 footprint tests with `node:test`;
  they need no Docker and no installed dependencies (the footprint test runs the in-image script on a temporary
  directory). Shutdown waits for in-flight topic setup and client-container
  cancellation before disposing attempt topics, including probes when `--keep-broker` retains the broker.
- ESLint covers `benchmarks/**/*.ts` with non-type-aware rules from the repository root (`pnpm lint`).
- Dependabot proposes competitor bumps monthly. Re-run and re-publish after merging one, and re-check what the
  docs page says each client implements (`CLIENT_COVERAGE` in `scripts/generate-benchmark-docs.mjs`). The docs
  generator, `--check` included, fails until `CLIENT_COVERAGE_CHECKED_VERSIONS` there matches the versions
  pinned in `package.json`.
- The security workflow scans only the repository's root lockfile. This lockfile is not scanned: these packages
  are never shipped, and at run time they execute only in containers without internet access. They are installed
  with network access during `docker build`, from the frozen lockfile with integrity hashes and pnpm's one-day
  `minimumReleaseAge`; only the Confluent package may run an install script, in a stage whose package store is
  not shared with the kafka-core build.
