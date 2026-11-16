import assert from 'node:assert/strict';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, it } from 'node:test';

import { resultSchemaVersion, scenarios } from '../client/spec.ts';
import type { ClientId, RunRecord, RunSpec, ScenarioId } from '../client/spec.ts';
import { aggregateSession, summarize } from './aggregate.ts';
import { findPublishBlockers, publishSession } from './publish.ts';
import { footprintTotal, formatNativeAddon, writeReport } from './report.ts';
import { validateSummary } from './schema.ts';
import type { EnvironmentReport, Footprint, SessionManifest } from './types.ts';

const sessionId = '20261112000000-abcdef123456';

function createManifest(overrides: Partial<SessionManifest> = {}): SessionManifest {
  return {
    sessionId,
    tier: 'standard',
    seed: 42,
    startedAt: '2026-11-12T00:00:00.000Z',
    finishedAt: '2026-11-12T01:00:00.000Z',
    repetitions: 2,
    clients: ['kafka-core', 'kafkajs'],
    runtimes: ['node24'],
    scenarios: ['P3'],
    profiles: ['normalized'],
    payload: 'small',
    countScale: 1,
    series: ['kafka-core', 'kafkajs'],
    core: {
      requestedRef: 'WORKTREE',
      sha: 'abcdef1234567890',
      commitDate: '2026-11-11T00:00:00Z',
      isWorktree: true,
      isDirty: false,
      isOnOriginDevelop: false,
    },
    images: [],
    broker: { image: 'apache/kafka:4.3.2', digest: null },
    plannedRuns: 4,
    ...overrides,
  };
}

function createEnvironment(warnings: string[]): EnvironmentReport {
  return {
    takenAt: '2026-11-12T00:00:00.000Z',
    host: { platform: 'darwin', arch: 'arm64', cpuModel: 'Apple M5', cpuCount: 10, memoryBytes: 1, osRelease: '27' },
    docker: { serverVersion: '29', vmCpus: 6, vmMemoryBytes: 1, kernel: 'linuxkit' },
    budget: {
      vmCpus: 6,
      vmMemoryBytes: 1,
      brokerCpuset: '0-2',
      brokerMemoryBytes: 1,
      brokerHeapBytes: 1,
      brokerTmpfsBytes: 1,
      clientCpuset: '3-4',
      clientMemoryBytes: 1,
    },
    checks: [],
    forced: warnings.length > 0,
    warnings,
  };
}

function createRun(
  client: ClientId,
  scenario: ScenarioId,
  repetition: number,
  recordsPerSecond: number,
  series: string = client,
): RunRecord {
  const runId = `${repetition}-${scenario}-${series.replace('#', '-')}-node24-normalized`;
  const spec: RunSpec = {
    runId,
    scenario,
    client,
    runtime: 'node24',
    profile: 'normalized',
    series,
    repetition,
    brokers: ['broker:9092'],
    topic: `bench-${runId}`,
    partitions: 3,
    groupId: `bench-${runId}`,
    payload: 'small',
    seed: 42,
    messageCount: 1000,
    warmupCount: 100,
    rateMessagesPerSecond: 0,
    durationMs: 0,
    wire: scenarios[scenario].wire,
    timeoutMs: 60_000,
  };
  return {
    spec,
    result: {
      schemaVersion: resultSchemaVersion,
      runId,
      status: 'ok',
      error: null,
      runtime: { id: 'node24', name: 'node', version: '24.22.3' },
      clientVersion: '1.0.0',
      capabilityGaps: client === 'kafkajs' ? ['lingerMs: no linger support'] : [],
      observed: { producedRecords: 1100, consumedRecords: 0, consumedBytes: 0 },
      metrics: {
        records: 1000,
        bytes: 100_000,
        elapsedMs: 1000,
        recordsPerSecond,
        megabytesPerSecond: 0.1,
        latency: { samples: 1000, p50Ms: 1, p95Ms: 2, p99Ms: 3, maxMs: 4 },
        cpu: { userSeconds: 0.5, systemSeconds: 0.1 },
        cgroupCpuSeconds: 0.7,
        recordsPerCpuSecond: recordsPerSecond / 0.6,
        peakMemoryBytes: 100_000_000,
        rssBytes: 90_000_000,
        heapUsedBytes: 10_000_000,
        gc: { count: 3, totalMs: 2 },
        eventLoopDelayP99Ms: 1,
        startup: null,
      },
    },
    verification: { passed: true, details: 'end offsets 1100 == 1100' },
    image: { reference: 'kafka-bench-client:node24', digest: null },
    startedAt: '2026-11-12T00:00:00.000Z',
    wallMs: 2000,
  };
}

async function writeSession(
  directory: string,
  runs: RunRecord[],
  warnings: string[],
  manifest: SessionManifest = createManifest(),
): Promise<void> {
  await mkdir(join(directory, 'runs'), { recursive: true });
  await writeFile(join(directory, 'session.json'), JSON.stringify(manifest));
  await writeFile(join(directory, 'environment.json'), JSON.stringify(createEnvironment(warnings)));
  for (const run of runs) {
    await writeFile(join(directory, 'runs', `${run.spec.runId}.json`), JSON.stringify(run));
  }
}

const temporaryDirs: string[] = [];

async function createSessionDir(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), 'kafka-bench-results-'));
  temporaryDirs.push(directory);
  return directory;
}

afterEach(async () => {
  await Promise.all(temporaryDirs.splice(0).map((directory) => rm(directory, { recursive: true, force: true })));
});

describe('summarize', () => {
  it('computes median, range and coefficient of variation', () => {
    assert.deepEqual(summarize([]), null);
    const stat = summarize([4, 1, 3, 2]);
    assert.equal(stat?.median, 2.5);
    assert.equal(stat?.min, 1);
    assert.equal(stat?.max, 4);
    assert.ok(Math.abs((stat?.cv ?? 0) - 0.5164) < 0.001);
  });
});

describe('aggregateSession', () => {
  it('compares series with kafka-core and only calls a winner when ranges separate', async () => {
    const directory = await createSessionDir();
    await writeSession(
      directory,
      [
        createRun('kafka-core', 'P3', 1, 100_000),
        createRun('kafka-core', 'P3', 2, 101_000),
        createRun('kafkajs', 'P3', 1, 50_000),
        createRun('kafkajs', 'P3', 2, 50_500),
      ],
      [],
    );

    const summary = await aggregateSession(directory);

    assert.equal(summary.cells.length, 2);
    assert.equal(summary.comparisons.length, 1);
    const [comparison] = summary.comparisons;
    assert.equal(comparison?.series, 'kafkajs');
    assert.equal(comparison?.verdict, 'worse');
    assert.ok(Math.abs((comparison?.ratio ?? 0) - 0.5) < 0.001);
    assert.deepEqual(await validateSummary(summary), []);
  });

  it('marks overlapping ranges inconclusive and records failed runs', async () => {
    const directory = await createSessionDir();
    const failed = createRun('kafkajs', 'P3', 3, 1);
    failed.verification = { passed: false, details: 'end offsets 900 != 1100' };
    await writeSession(
      directory,
      [
        createRun('kafka-core', 'P3', 1, 100_000),
        createRun('kafka-core', 'P3', 2, 101_000),
        createRun('kafkajs', 'P3', 1, 100_500),
        createRun('kafkajs', 'P3', 2, 100_800),
        failed,
      ],
      [],
    );

    const summary = await aggregateSession(directory);

    assert.equal(summary.comparisons[0]?.verdict, 'inconclusive');
    assert.equal(summary.failures.length, 1);
    assert.equal(summary.failures[0]?.reason, 'end offsets 900 != 1100');
  });
});

describe('E1 latency comparisons', () => {
  function createE1Run(series: string, repetition: number, p50Ms: number, p99Ms: number): RunRecord {
    const client: ClientId = series === 'kafkajs' ? 'kafkajs' : 'kafka-core';
    const run = createRun(client, 'E1', repetition, 5000, series);
    const { metrics } = run.result;
    assert.ok(metrics);
    metrics.latency = { samples: 1000, p50Ms, p95Ms: p99Ms, p99Ms, maxMs: p99Ms };
    return run;
  }

  it('compares the median as well as p99 and names the metric of each comparison', async () => {
    const directory = await createSessionDir();
    await writeSession(
      directory,
      [
        createE1Run('kafka-core', 1, 1.0, 3.0),
        createE1Run('kafka-core', 2, 1.01, 3.02),
        createE1Run('kafkajs', 1, 1.4, 3.01),
        createE1Run('kafkajs', 2, 1.41, 3.03),
      ],
      [],
    );

    const summary = await aggregateSession(directory);

    const verdicts = Object.fromEntries(summary.comparisons.map((comparison) => [comparison.metric, comparison]));
    assert.deepEqual(Object.keys(verdicts).sort(), ['latencyP50Ms', 'latencyP99Ms']);
    assert.equal(verdicts.latencyP50Ms?.verdict, 'worse');
    assert.equal(verdicts.latencyP99Ms?.verdict, 'inconclusive');
    assert.equal(verdicts.latencyP99Ms?.reason, 'min-max ranges overlap');
    assert.deepEqual(await validateSummary(summary), []);
    const report = await readFile(await writeReport(directory, summary), 'utf8');
    assert.match(report, /\| p99 1\.00× \(inconclusive\); p50 1\.40× \(worse\) \|/);
  });

  it('applies the A/A floor of the median, not of p99, to the median', async () => {
    const directory = await createSessionDir();
    await writeSession(
      directory,
      [
        createE1Run('kafka-core', 1, 1.0, 3.0),
        createE1Run('kafka-core', 2, 1.0, 3.0),
        createE1Run('kafka-core#2', 1, 1.02, 4.0),
        createE1Run('kafka-core#2', 2, 1.02, 4.0),
        createE1Run('kafkajs', 1, 1.1, 3.6),
        createE1Run('kafkajs', 2, 1.1, 3.6),
      ],
      [],
    );

    const summary = await aggregateSession(directory);

    assert.ok(Math.abs((summary.aaNoiseFloors.E1?.latencyP50Ms ?? 0) - 0.02) < 0.001);
    assert.ok(Math.abs((summary.aaNoiseFloors.E1?.latencyP99Ms ?? 0) - 0.3333) < 0.001);
    const verdicts = Object.fromEntries(summary.comparisons.map((comparison) => [comparison.metric, comparison]));
    assert.equal(verdicts.latencyP50Ms?.verdict, 'worse');
    assert.equal(verdicts.latencyP99Ms?.verdict, 'inconclusive');
    assert.equal(verdicts.latencyP99Ms?.reason, 'difference is within the A/A noise floor');
    assert.deepEqual(await validateSummary(summary), []);
  });
});

describe('noise floors', () => {
  it('measures the A/A floor per scenario and does not compare the A/A series itself', async () => {
    const directory = await createSessionDir();
    await writeSession(
      directory,
      [
        createRun('kafka-core', 'P3', 1, 100_000),
        createRun('kafka-core', 'P3', 2, 100_400),
        createRun('kafka-core', 'P3', 1, 92_000, 'kafka-core#2'),
        createRun('kafka-core', 'P3', 2, 92_400, 'kafka-core#2'),
        createRun('kafkajs', 'P3', 1, 95_000),
        createRun('kafkajs', 'P3', 2, 95_400),
      ],
      [],
    );

    const summary = await aggregateSession(directory);

    assert.ok(Math.abs((summary.aaNoiseFloors.P3?.recordsPerSecond ?? 0) - 0.08) < 0.001);
    assert.deepEqual(
      summary.comparisons.map((comparison) => comparison.series),
      ['kafkajs'],
    );
    assert.equal(summary.comparisons[0]?.verdict, 'inconclusive');
    assert.equal(summary.comparisons[0]?.reason, 'difference is within the A/A noise floor');
  });

  it('applies a noise floor passed in from calibration', async () => {
    const directory = await createSessionDir();
    await writeSession(
      directory,
      [
        createRun('kafka-core', 'P3', 1, 100_000),
        createRun('kafka-core', 'P3', 2, 100_400),
        createRun('kafkajs', 'P3', 1, 97_000),
        createRun('kafkajs', 'P3', 2, 97_400),
      ],
      [],
    );

    const withoutFloor = await aggregateSession(directory);
    const withFloor = await aggregateSession(directory, { noiseFloor: 0.05 });

    assert.equal(withoutFloor.comparisons[0]?.verdict, 'worse');
    assert.equal(withFloor.comparisons[0]?.verdict, 'inconclusive');
    assert.equal(withFloor.noiseFloor, 0.05);
  });
});

describe('writeReport', () => {
  it('renders a Markdown table with capability footnotes', async () => {
    const directory = await createSessionDir();
    await writeSession(
      directory,
      [createRun('kafka-core', 'P3', 1, 100_000), createRun('kafkajs', 'P3', 1, 50_000)],
      ['Low Power Mode is off'],
    );

    const report = await readFile(await writeReport(directory), 'utf8');

    assert.match(
      report,
      /### P3 `produce-throughput`\n\nStructurally biased: .* Use P2 as the headline produce comparison\./,
    );
    assert.match(report, /\| node24 \| normalized \| kafkajs\[\^1\] \| 50,000 \|/);
    assert.match(report, /\[\^1\]: lingerMs: no linger support/);
    assert.match(report, /Doctor warnings/);
  });
});

describe('F1 footprints', () => {
  const footprints: Footprint[] = [
    {
      client: 'kafka-core',
      packageName: '@cookiemonsterdev/kafka-core',
      version: '2.8.1',
      required: { bytes: 2_155_686, fileCount: 891, dependencyCount: 3, hasNativeAddon: false },
      optional: { bytes: 538_332, fileCount: 9, dependencyCount: 2, hasNativeAddon: true },
    },
    {
      client: 'confluent',
      packageName: '@confluentinc/kafka-javascript',
      version: null,
      required: null,
      optional: null,
    },
  ];

  it('keeps required and optional parts in the summary and renders them without a verdict', async () => {
    const directory = await createSessionDir();
    await writeSession(directory, [createRun('kafka-core', 'P3', 1, 100_000)], []);
    await writeFile(join(directory, 'footprint.json'), JSON.stringify(footprints));

    const summary = await aggregateSession(directory);
    const report = await readFile(await writeReport(directory, summary), 'utf8');

    assert.deepEqual(summary.footprints, footprints);
    assert.deepEqual(await validateSummary(summary), []);
    assert.match(
      report,
      /\| @cookiemonsterdev\/kafka-core \| 2\.8\.1 \| 2\.1 \| 3 \| 891 \| 0\.5 \| 2 \| 9 \| optional only \|/,
    );
    assert.match(
      report,
      /\| @confluentinc\/kafka-javascript \| n\/a \| n\/a \| n\/a \| n\/a \| n\/a \| n\/a \| n\/a \| n\/a \|/,
    );
  });

  it('keeps only the total of footprints recorded before the required and optional split', async () => {
    const directory = await createSessionDir();
    await writeSession(directory, [createRun('kafka-core', 'P3', 1, 100_000)], []);
    const unsplit = [
      {
        client: 'kafka-core',
        packageName: '@cookiemonsterdev/kafka-core',
        version: '2.8.1',
        installBytes: 2_694_018,
        dependencyCount: 5,
        hasNativeAddon: true,
      },
      {
        client: 'confluent',
        packageName: '@confluentinc/kafka-javascript',
        version: null,
        installBytes: null,
        dependencyCount: null,
        hasNativeAddon: null,
      },
    ];
    await writeFile(join(directory, 'footprint.json'), JSON.stringify(unsplit));

    const summary = await aggregateSession(directory);
    const report = await readFile(await writeReport(directory, summary), 'utf8');

    assert.deepEqual(summary.footprints, [
      {
        client: 'kafka-core',
        packageName: '@cookiemonsterdev/kafka-core',
        version: '2.8.1',
        required: null,
        optional: null,
        unsplitTotal: { bytes: 2_694_018, dependencyCount: 5, hasNativeAddon: true },
      },
      footprints[1],
    ]);
    assert.deepEqual(await validateSummary(summary), []);
    assert.match(report, /\| Version \| Total MiB \| Total dependencies \| Required MiB \|/);
    assert.match(
      report,
      /\| @cookiemonsterdev\/kafka-core \| 2\.8\.1 \| 2\.6 \| 5 \|( not split \|){6} yes, not split \|/,
    );
    assert.match(report, /\| @confluentinc\/kafka-javascript \| n\/a \|( n\/a \|){9}/);
  });

  it('totals the required and optional parts like an unsplit footprint', () => {
    const [split, notInstalled] = footprints;
    assert.ok(split && notInstalled);

    assert.deepEqual(footprintTotal(split), { bytes: 2_694_018, dependencyCount: 5 });
    assert.equal(footprintTotal(notInstalled), null);
    assert.equal(formatNativeAddon(split), 'optional only');
  });
});

describe('publishSession', () => {
  it('rejects a session with doctor warnings, a worktree core and too few repetitions', async () => {
    const directory = await createSessionDir();
    await writeSession(directory, [createRun('kafka-core', 'P3', 1, 100_000)], ['Low Power Mode is off']);
    const options = { publishedDir: join(directory, 'published'), repoRoot: directory, minRepetitions: 5 };

    const blockers = await findPublishBlockers(await aggregateSession(directory), options);

    assert.equal(blockers.length, 4);
    assert.match(blockers.join('\n'), /doctor reported warnings/);
    assert.match(blockers.join('\n'), /working tree/);
    await assert.rejects(publishSession(directory, options), /cannot be published/);
  });

  it('rejects smoke and A/A sessions', async () => {
    const directory = await createSessionDir();
    const manifest = createManifest({ tier: 'smoke', series: ['kafka-core', 'kafka-core#2'] });
    await writeSession(directory, [createRun('kafka-core', 'P3', 1, 100_000)], [], manifest);
    const options = { publishedDir: join(directory, 'published'), repoRoot: directory, minRepetitions: 1 };

    const blockers = (await findPublishBlockers(await aggregateSession(directory), options)).join('\n');

    assert.match(blockers, /smoke sessions only check the harness/);
    assert.match(blockers, /A\/A sessions measure the noise floor/);
  });
});
