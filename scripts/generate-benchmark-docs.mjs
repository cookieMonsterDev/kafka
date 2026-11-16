#!/usr/bin/env node
// Generates the docs site's client benchmark page from saved benchmark sessions
// (`benchmarks/results/raw/<session>/`) in two steps that always run together:
//
//   1. With session directories: builds one dataset (the numbers the page shows, the provenance of each
//      one, and the workload the session ran) and stores it in
//      `packages/docs/src/data/benchmarks/comparison.json` under the session's payload. Datasets of the
//      other payload are kept, so the 1 KiB and 100 B results can be re-measured separately. Without
//      session directories, the committed JSON is reused, so a layout change needs no session data.
//   2. Writes the whole page, `packages/docs/src/content/docs/core/reference/benchmarks.md`: the setup
//      (payloads, record counts, wire settings, environment), then one ```chart block (ApexCharts options
//      as JSON, rendered by `packages/docs/src/lib/rehype-chart.ts`) and one table per entry in `CHARTS`,
//      with both payloads side by side. Change the page here, not in the Markdown file.
//
// Usage:
//   node scripts/generate-benchmark-docs.mjs --competitors <session dir> --core <session dir>
//        [--core-fallback <session dir>]
//   node scripts/generate-benchmark-docs.mjs            # re-render from the committed JSON
//   node scripts/generate-benchmark-docs.mjs --check    # fail if any output is out of date
//
// Competitor cells come from `--competitors`. kafka-core cells come from `--core`, and cells that session
// did not measure come from `--core-fallback` (default: the `--competitors` session, which also measures
// kafka-core). Pass the same directory to both for a same-session run. All sessions of one dataset must
// use the same payload and record counts. Output is deterministic: no timestamps beyond the sessions'
// own, fixed ordering and fixed decimals.
import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';

// The benchmark suite's own definitions, so the page describes exactly the bytes and scenarios it runs.
import { createRecordPool, headerValueBytes, payloadShapes } from '../benchmarks/client/payload.ts';
import { payloadSeed, scenarioIds, scenarios as scenarioDefinitions } from '../benchmarks/client/spec.ts';
import { effectiveNoiseFloor, findClearBest, summarize, summarySchemaVersion } from '../benchmarks/runner/aggregate.ts';

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
const DATA_PATH = join(ROOT, 'packages/docs/src/data/benchmarks/comparison.json');
const PAGE_PATH = join(ROOT, 'packages/docs/src/content/docs/core/reference/benchmarks.md');
const BENCHMARKS_README_URL = 'https://github.com/cookieMonsterDev/kafka/blob/develop/benchmarks/README.md';
const NEW_ISSUE_URL = 'https://github.com/cookieMonsterDev/kafka/issues/new/choose';
const SCHEMA_VERSION = 3;

/** Payloads in page order: the headline first. `id` is the runner's `--payload` value. */
const PAYLOADS = [
  { id: 'medium', label: '1 KiB', role: 'Headline' },
  { id: 'small', label: '100 B', role: 'Per-record overhead stress test' },
];
const [HEADLINE_PAYLOAD, STRESS_PAYLOAD] = PAYLOADS;

const CLIENTS = [
  { id: 'kafka-core', label: 'kafka-core', packageName: '@cookiemonsterdev/kafka-core' },
  { id: 'platformatic', label: 'Platformatic', packageName: '@platformatic/kafka' },
  { id: 'kafkajs', label: 'KafkaJS', packageName: 'kafkajs' },
  { id: 'confluent', label: 'Confluent', packageName: '@confluentinc/kafka-javascript' },
];
const CORE_CLIENT_ID = 'kafka-core';
const RUNTIMES = [
  { id: 'node24', label: 'Node.js 24' },
  { id: 'node26', label: 'Node.js 26' },
  { id: 'bun', label: 'Bun' },
];

const MEBIBYTE = 1024 * 1024;
const METRICS = {
  recordsPerSecond: { unit: 'records/s', axisTitle: 'records per second', decimals: 0 },
  latencyP50Ms: { unit: 'ms', axisTitle: 'milliseconds', decimals: 3 },
  latencyP99Ms: { unit: 'ms', axisTitle: 'milliseconds', decimals: 3 },
  firstAckedProduceMs: { unit: 'ms', axisTitle: 'milliseconds from process start', decimals: 1 },
  peakMemoryBytes: { unit: 'MiB', axisTitle: 'MiB (peak container memory)', decimals: 0, divisor: MEBIBYTE },
  installBytes: { unit: 'MB', axisTitle: 'megabytes (1 MB = 1,000,000 bytes)', decimals: 0, divisor: 1_000_000 },
};

/**
 * Every chart on the page, in page order. `id` is the SVG file name, `section` the page's `##` heading,
 * `heading` its `###` heading (omitted when the section has one chart), `name` the chart's plain name, and
 * `description` the one line shown above the chart.
 */
const CHARTS = [
  {
    id: 'batched-send',
    section: 'Produce',
    heading: 'Batched send (100 records)',
    name: 'Batched send (100 records)',
    scenario: 'P2',
    profile: 'normalized',
    metric: 'recordsPerSecond',
    better: 'higher',
    description: 'Each call awaits `send()` of 100 records with acks 1. This is the fairest produce comparison.',
  },
  {
    id: 'single-record-send',
    section: 'Produce',
    heading: 'Single-record send',
    name: 'Single-record send',
    scenario: 'P1',
    profile: 'normalized',
    metric: 'recordsPerSecond',
    better: 'higher',
    description:
      'Each call awaits `send()` of one record with acks 1 and no linger, so every record waits for a broker round trip.',
  },
  {
    id: 'concurrent-sends',
    section: 'Produce',
    heading: 'Concurrent sends',
    name: 'Concurrent sends',
    scenario: 'P3',
    profile: 'normalized',
    metric: 'recordsPerSecond',
    better: 'higher',
    description:
      'Up to 256 concurrent single-record `send()` calls with linger 5 ms. This favours clients that merge separate calls into one batch, as kafka-core does.',
  },
  {
    id: 'consume-per-message',
    section: 'Consume',
    heading: 'Per message',
    name: 'Consume (per message)',
    scenario: 'C1',
    profile: 'normalized',
    metric: 'recordsPerSecond',
    better: 'higher',
    description:
      'Reads 2,000,000 preloaded records with a per-record handler and the same fetch settings for every client.',
  },
  {
    id: 'consume-per-message-defaults',
    section: 'Consume',
    heading: 'Per message, library defaults',
    name: 'Consume (per message, library defaults)',
    scenario: 'C1',
    profile: 'defaults',
    metric: 'recordsPerSecond',
    better: 'higher',
    description: "The same read with each library's default settings, including its own auto-commit.",
  },
  {
    id: 'consume-batch',
    section: 'Consume',
    heading: 'Batch',
    name: 'Consume (batch)',
    scenario: 'C2',
    profile: 'normalized',
    metric: 'recordsPerSecond',
    better: 'higher',
    description: "The same read through each client's batch API. Platformatic has none, so its adapter emulates one.",
  },
  {
    id: 'latency-median',
    section: 'End-to-end latency',
    heading: 'Median',
    name: 'End-to-end latency, median',
    scenario: 'E1',
    profile: 'normalized',
    metric: 'latencyP50Ms',
    better: 'lower',
    description:
      'A producer sends 5,000 records per second for 15 seconds while a consumer reads them. Latency runs from the scheduled send time to consumption.',
  },
  {
    id: 'latency-p99',
    section: 'End-to-end latency',
    heading: '99th percentile',
    name: 'End-to-end latency, p99',
    scenario: 'E1',
    profile: 'normalized',
    metric: 'latencyP99Ms',
    better: 'lower',
    description: 'The same runs, 99th percentile.',
  },
  {
    id: 'startup',
    section: 'Startup',
    name: 'Startup, first acknowledged produce',
    scenario: 'L1',
    profile: 'normalized',
    metric: 'firstAckedProduceMs',
    better: 'lower',
    description: 'Time from process start to the first acknowledged produce.',
  },
  {
    id: 'memory-batched-send',
    section: 'Peak memory',
    heading: 'Batched send',
    name: 'Peak memory, batched send',
    scenario: 'P2',
    profile: 'normalized',
    metric: 'peakMemoryBytes',
    better: 'lower',
    description: 'Peak container memory during batched send, under a 2 GiB limit.',
  },
  {
    id: 'memory-consume-per-message',
    section: 'Peak memory',
    heading: 'Consume (per message)',
    name: 'Peak memory, consume (per message)',
    scenario: 'C1',
    profile: 'normalized',
    metric: 'peakMemoryBytes',
    better: 'lower',
    description: 'Peak container memory while consuming per message, under a 2 GiB limit.',
  },
  {
    id: 'memory-consume-batch',
    section: 'Peak memory',
    heading: 'Consume (batch)',
    name: 'Peak memory, consume (batch)',
    scenario: 'C2',
    profile: 'normalized',
    metric: 'peakMemoryBytes',
    better: 'lower',
    description: 'Peak container memory while consuming in batches, under a 2 GiB limit.',
  },
  {
    id: 'install-size',
    section: 'Install size',
    name: 'Install size',
    scenario: 'F1',
    profile: null,
    metric: 'installBytes',
    better: 'lower',
    description: 'Size of the package and its production dependencies as installed.',
  },
];

// ---------------------------------------------------------------------------------------------------
// Step 1: session directories → comparison.json

function readJson(path) {
  return JSON.parse(readFileSync(path, 'utf8'));
}

/** Loads the files of one session directory that the comparison needs. */
function loadSession(directory) {
  const sessionDir = resolve(directory);
  const summaryPath = join(sessionDir, 'summary.json');
  if (!existsSync(summaryPath)) throw new Error(`${sessionDir} has no summary.json; is it a session directory?`);
  const summary = readJson(summaryPath);
  if (summary.schemaVersion !== summarySchemaVersion) {
    throw new Error(
      `${summaryPath} has schemaVersion ${summary.schemaVersion}; re-aggregate it with pnpm benchmarks:report --session ${sessionDir}, repeating any --noise-threshold or --noise-floor it was aggregated with`,
    );
  }
  const runtimeVersions = Object.fromEntries(
    summary.manifest.images.map((image) => [image.runtime, image.runtimeVersion]),
  );
  const runsDir = join(sessionDir, 'runs');
  const runs = readdirSync(runsDir)
    .filter((name) => name.endsWith('.json'))
    .sort()
    .map((name) => readJson(join(runsDir, name)));
  return { summary, runtimeVersions, runs };
}

function describeSession({ summary, runtimeVersions }) {
  const { manifest, environment } = summary;
  return {
    sessionId: summary.sessionId,
    tier: manifest.tier,
    seed: manifest.seed,
    payload: manifest.payload,
    countScale: manifest.countScale,
    repetitions: manifest.repetitions,
    startedAt: manifest.startedAt,
    finishedAt: manifest.finishedAt,
    scenarios: manifest.scenarios,
    clients: manifest.clients,
    coreSha: manifest.core.sha,
    coreRequestedRef: manifest.core.requestedRef,
    coreIsOnOriginDevelop: manifest.core.isOnOriginDevelop,
    runtimeVersions,
    clientVersions: manifest.images[0]?.clientVersions ?? {},
    brokerImage: manifest.broker.image,
    brokerDigest: manifest.broker.digest,
    doctorForced: environment.forced,
    doctorWarnings: environment.checks
      .filter((check) => check.status !== 'pass')
      .map((check) => ({ name: check.name, details: check.details })),
  };
}

function round(value, decimals) {
  if (value === null || value === undefined) return null;
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
}

function findCell(session, chart, runtime, client) {
  return session.summary.cells.find(
    (cell) =>
      cell.scenario === chart.scenario &&
      cell.profile === chart.profile &&
      cell.runtime === runtime &&
      cell.series === client,
  );
}

function isUnsupported(session, runtime, client) {
  return session.summary.supportMatrix.cells.some(
    (cell) => cell.runtime === runtime && cell.client === client && !cell.supported,
  );
}

function describeFailure(failure) {
  const repetition = failure.runId.split('-r').at(-1);
  const exitCode = /exit code (\d+)/.exec(failure.reason)?.[1];
  if (exitCode === '137') return `repetition ${repetition}: killed (exit code 137)`;
  return `repetition ${repetition}: ${failure.reason}`;
}

function sessionCandidates(client, sources) {
  if (client !== CORE_CLIENT_ID) return [['competitors', sources.competitors]];
  return [
    ['core', sources.core],
    ['core-fallback', sources.coreFallback],
  ];
}

/** One chart cell with its provenance, or an explicit unsupported / failed / not-measured marker. */
function buildCell({ chart, runtime, client, sources }) {
  const competitorSession = sources.competitors;
  if (client !== CORE_CLIENT_ID && isUnsupported(competitorSession, runtime, client)) {
    return {
      runtime,
      client,
      status: 'unsupported',
      source: 'competitors',
      sessionId: competitorSession.summary.sessionId,
    };
  }
  for (const [source, session] of sessionCandidates(client, sources)) {
    const cell = findCell(session, chart, runtime, client);
    if (cell === undefined) continue;
    const metric = cell.metrics[chart.metric];
    const { decimals } = METRICS[chart.metric];
    const failures = session.summary.failures
      .filter(
        (failure) =>
          failure.scenario === chart.scenario &&
          failure.profile === chart.profile &&
          failure.runtime === runtime &&
          failure.series === client,
      )
      .map(describeFailure)
      .sort();
    if (cell.passedRuns === 0) {
      return {
        runtime,
        client,
        status: 'failed',
        source,
        sessionId: session.summary.sessionId,
        runs: cell.runs,
        passedRuns: 0,
        failures,
      };
    }
    return {
      runtime,
      client,
      status: cell.passedRuns < cell.runs ? 'partial' : 'ok',
      source,
      sessionId: session.summary.sessionId,
      ...(client === CORE_CLIENT_ID ? { coreSha: session.summary.manifest.core.sha } : {}),
      clientVersion: cell.clientVersion,
      runtimeVersion: session.runtimeVersions[runtime] ?? null,
      runs: cell.runs,
      passedRuns: cell.passedRuns,
      median: round(metric.median, decimals),
      min: round(metric.min, decimals),
      max: round(metric.max, decimals),
      cv: round(metric.cv, 4),
      ...(failures.length > 0 ? { failures } : {}),
    };
  }
  return { runtime, client, status: 'not-measured' };
}

/**
 * The comparison rule's settings for one chart, the strictest across the sessions it draws from: the lowest
 * noise threshold, and the largest noise floor (calibrated, or A/A for the chart's scenario and metric).
 */
function comparisonSettings(chart, sources) {
  const summaries = Object.values(sources).map((session) => session.summary);
  return {
    noiseThresholdCv: Math.min(...summaries.map((summary) => summary.noiseThresholdCv)),
    noiseFloor: Math.max(...summaries.map((summary) => effectiveNoiseFloor(summary, chart.scenario, chart.metric))),
  };
}

/** Settings each adapter declared it could not express for this scenario, keyed by client. */
function declaredGaps(chart, sources) {
  const gapsByClient = {};
  for (const client of CLIENTS) {
    const session = client.id === CORE_CLIENT_ID ? sources.core : sources.competitors;
    const gaps = new Set(
      session.summary.cells
        .filter(
          (cell) => cell.scenario === chart.scenario && cell.profile === chart.profile && cell.series === client.id,
        )
        .flatMap((cell) => cell.capabilityGaps),
    );
    if (gaps.size > 0) gapsByClient[client.id] = [...gaps].sort();
  }
  return gapsByClient;
}

function buildFootprints(sources) {
  return CLIENTS.map(({ id }) => {
    for (const [source, session] of sessionCandidates(id, sources)) {
      const footprint = session.summary.footprints.find((entry) => entry.client === id);
      if (footprint === undefined || footprint.installBytes === null) continue;
      return {
        client: id,
        status: 'ok',
        packageName: footprint.packageName,
        version: footprint.version,
        installBytes: footprint.installBytes,
        dependencyCount: footprint.dependencyCount,
        hasNativeAddon: footprint.hasNativeAddon,
        source,
        sessionId: session.summary.sessionId,
        ...(id === CORE_CLIENT_ID ? { coreSha: session.summary.manifest.core.sha } : {}),
      };
    }
    return { client: id, status: 'not-measured' };
  });
}

/** Median elapsed seconds of each (client, runtime) under the normalized profile, from passed runs. */
function medianElapsedSeconds(session, scenario, clientIds) {
  const elapsedByCell = new Map();
  for (const { spec, result, verification } of session.runs) {
    if (spec.scenario !== scenario || spec.profile !== 'normalized' || !clientIds.includes(spec.series)) continue;
    if (!verification.passed || result.metrics === null) continue;
    const key = `${spec.series}/${spec.runtime}`;
    if (!elapsedByCell.has(key)) elapsedByCell.set(key, []);
    elapsedByCell.get(key).push(result.metrics.elapsedMs / 1000);
  }
  return [...elapsedByCell.values()].map((values) => summarize(values).median);
}

function countsOf(spec) {
  const { messageCount, warmupCount, rateMessagesPerSecond, durationMs } = spec;
  return { messageCount, warmupCount, rateMessagesPerSecond, durationMs };
}

/**
 * What each scenario ran: record counts, warm-up and wire settings as the runner passed them, and the
 * measured time of the fastest and slowest (client, runtime) whose cells the page shows.
 */
function buildWorkload(sources) {
  const competitorIds = CLIENTS.map((client) => client.id).filter((id) => id !== CORE_CLIENT_ID);
  return scenarioIds
    .filter((scenario) => sources.competitors.summary.manifest.scenarios.includes(scenario))
    .map((scenario) => {
      const spec = sources.competitors.runs.find(
        (run) => run.spec.scenario === scenario && run.spec.profile === 'normalized',
      )?.spec;
      if (spec === undefined) throw new Error(`The competitors session has no normalized ${scenario} run`);
      const counts = countsOf(spec);
      for (const [source, session] of Object.entries(sources)) {
        const other = session.runs.find((run) => run.spec.scenario === scenario)?.spec;
        if (other !== undefined && JSON.stringify(countsOf(other)) !== JSON.stringify(counts)) {
          throw new Error(
            `The ${source} session ran ${scenario} with other record counts than the competitors session`,
          );
        }
      }
      // E1 runs for a fixed duration and L1 times one record, so neither has a measured window to show.
      const { kind } = scenarioDefinitions[scenario];
      const elapsed =
        kind === 'produce' || kind === 'consume'
          ? [
              ...medianElapsedSeconds(sources.competitors, scenario, competitorIds),
              ...medianElapsedSeconds(sources.core, scenario, [CORE_CLIENT_ID]),
            ]
          : [];
      return {
        scenario,
        ...counts,
        partitions: spec.partitions,
        wire: spec.wire,
        measuredSeconds:
          elapsed.length === 0
            ? null
            : { fastest: round(Math.min(...elapsed), 1), slowest: round(Math.max(...elapsed), 1) },
      };
    });
}

/** One payload's results: provenance, environment, workload, chart cells and footprints. */
function buildDataset(sources) {
  const { environment, manifest } = sources.competitors.summary;
  for (const [source, session] of Object.entries(sources)) {
    if (session.summary.manifest.payload !== manifest.payload) {
      throw new Error(
        `The ${source} session used the ${session.summary.manifest.payload} payload, the competitors session ${manifest.payload}`,
      );
    }
  }
  const charts = CHARTS.filter((chart) => chart.scenario !== 'F1').map((chart) => ({
    id: chart.id,
    scenario: chart.scenario,
    profile: chart.profile,
    metric: chart.metric,
    unit: METRICS[chart.metric].unit,
    better: chart.better,
    ...comparisonSettings(chart, sources),
    declaredGaps: declaredGaps(chart, sources),
    cells: RUNTIMES.flatMap((runtime) =>
      CLIENTS.map((client) => buildCell({ chart, runtime: runtime.id, client: client.id, sources })),
    ),
  }));
  return {
    payload: manifest.payload,
    sessions: {
      competitors: describeSession(sources.competitors),
      core: describeSession(sources.core),
      coreFallback: describeSession(sources.coreFallback),
    },
    environment: {
      cpuModel: environment.host.cpuModel,
      cpuCount: environment.host.cpuCount,
      memoryBytes: environment.host.memoryBytes,
      platform: environment.host.platform,
      arch: environment.host.arch,
      osRelease: environment.host.osRelease,
      dockerServerVersion: environment.docker.serverVersion,
      dockerVmCpus: environment.docker.vmCpus,
      dockerVmMemoryBytes: environment.docker.vmMemoryBytes,
      brokerCpuset: environment.budget.brokerCpuset,
      brokerMemoryBytes: environment.budget.brokerMemoryBytes,
      brokerHeapBytes: environment.budget.brokerHeapBytes,
      brokerTmpfsBytes: environment.budget.brokerTmpfsBytes,
      clientCpuset: environment.budget.clientCpuset,
      clientMemoryBytes: environment.budget.clientMemoryBytes,
      brokerImage: manifest.broker.image,
    },
    workload: buildWorkload(sources),
    charts,
    footprints: buildFootprints(sources),
  };
}

// ---------------------------------------------------------------------------------------------------
// Formatting

function groupThousands(integerText) {
  return integerText.replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

/** Full-precision table value in the metric's display unit. */
function formatTableValue(value, metric) {
  const { divisor = 1 } = METRICS[metric];
  const scaled = value / divisor;
  if (metric === 'recordsPerSecond' || metric === 'peakMemoryBytes') return groupThousands(scaled.toFixed(0));
  if (metric === 'firstAckedProduceMs') return scaled.toFixed(1);
  return scaled.toFixed(2);
}

/** Short chart label: 709.5k, 2.84M, 3.42 ms, 512 MiB. */
function formatChartValue(value, metric) {
  const { divisor = 1, unit } = METRICS[metric];
  const scaled = value / divisor;
  if (metric === 'recordsPerSecond') {
    if (scaled >= 1_000_000) return `${(scaled / 1_000_000).toFixed(2)}M`;
    if (scaled >= 1_000) return `${(scaled / 1_000).toFixed(1)}k`;
    return scaled.toFixed(0);
  }
  if (metric === 'peakMemoryBytes' || metric === 'firstAckedProduceMs') return `${scaled.toFixed(0)} ${unit}`;
  if (metric === 'installBytes') return `${scaled.toFixed(1)} ${unit}`;
  return `${scaled.toFixed(2)} ${unit}`;
}

function trimZeros(text) {
  return text.includes('.') ? text.replace(/\.?0+$/, '') : text;
}

function clientLabel(clientId) {
  return CLIENTS.find((client) => client.id === clientId).label;
}

function runtimeLabel(runtimeId) {
  return RUNTIMES.find((runtime) => runtime.id === runtimeId).label;
}

function payloadLabel(payloadId) {
  return PAYLOADS.find((payload) => payload.id === payloadId).label;
}

function formatBytesAsGiB(bytes) {
  return `${trimZeros((bytes / 1024 ** 3).toFixed(1))} GiB`;
}

function formatCount(count) {
  return groupThousands(String(count));
}

/** 16 KiB, 50 MiB, 1 B: the wire settings are all whole powers of 1024 or tiny. */
function formatSettingBytes(bytes) {
  if (bytes >= MEBIBYTE && bytes % MEBIBYTE === 0) return `${bytes / MEBIBYTE} MiB`;
  if (bytes >= 1024 && bytes % 1024 === 0) return `${bytes / 1024} KiB`;
  return `${bytes} B`;
}

/** `0-2` → 3, `5` → 1. */
function cpusetSize(cpuset) {
  const [first, last = first] = cpuset.split('-').map(Number);
  return last - first + 1;
}

function isMeasured(row) {
  return row.status === 'ok' || row.status === 'partial';
}

/** kafka-core cells taken from an older build than the dataset's main kafka-core session. */
function isEarlierCoreBuild(row, dataset) {
  return (
    row.client === CORE_CLIENT_ID &&
    row.source === 'core-fallback' &&
    row.coreSha !== undefined &&
    row.coreSha !== dataset.sessions.core.coreSha
  );
}

/** Short enough for a 320 px wide chart; the page heading above names the chart. */
function chartTitle(chart) {
  const direction = chart.better === 'higher' ? 'Higher' : 'Lower';
  return `${direction} is better (${METRICS[chart.metric].unit})`;
}

// ---------------------------------------------------------------------------------------------------
// Step 2: comparison.json → the Markdown page

/** The stored datasets in page order, headline payload first. */
function orderedDatasets(comparison) {
  return PAYLOADS.map((payload) => comparison.datasets[payload.id]).filter((dataset) => dataset !== undefined);
}

function chartData(chart, dataset) {
  return dataset.charts.find((entry) => entry.id === chart.id);
}

/** Whether the dataset has anything to show for the chart: a value, or runs that failed. */
function hasResults(chart, dataset) {
  if (chart.scenario === 'F1') return dataset.footprints.some((footprint) => footprint.status === 'ok');
  return chartData(chart, dataset).cells.some((cell) => isMeasured(cell) || cell.status === 'failed');
}

/** Rows of one chart, grouped by runtime (or one group for install size). */
function chartGroups(chart, dataset) {
  if (chart.scenario === 'F1') {
    return [
      {
        label: 'Package and production dependencies',
        rows: dataset.footprints.map((footprint) => ({
          ...footprint,
          median: footprint.installBytes,
          min: footprint.installBytes,
          max: footprint.installBytes,
          cv: 0,
        })),
      },
    ];
  }
  return RUNTIMES.map((runtime) => ({
    label: runtimeLabel(runtime.id),
    rows: chartData(chart, dataset).cells.filter((cell) => cell.runtime === runtime.id),
  }));
}

function markdownTable(header, rows) {
  const widths = header.map((cell, column) => Math.max(cell.length, ...rows.map((row) => row[column].length)));
  const line = (cells) => `| ${cells.map((cell, column) => cell.padEnd(widths[column])).join(' | ')} |`;
  return [line(header), `| ${widths.map((width) => '-'.repeat(width)).join(' | ')} |`, ...rows.map(line)].join('\n');
}

/** The comparison rule for a chart; install size is one exact measurement per client, so any gap counts. */
function comparisonRule(chart, dataset) {
  const isLowerBetter = chart.better === 'lower';
  if (chart.scenario === 'F1') return { noiseThresholdCv: 0, noiseFloor: 0, isLowerBetter };
  const { noiseThresholdCv, noiseFloor } = chartData(chart, dataset);
  return { noiseThresholdCv, noiseFloor, isLowerBetter };
}

/** Per group, the client the comparison rule puts ahead of every other one, or "no clear highest/lowest". */
function bestPerGroupText(chart, dataset) {
  const rule = comparisonRule(chart, dataset);
  const noClearBestText = `no clear ${chart.better === 'higher' ? 'highest' : 'lowest'}`;
  const unitSuffix = chart.metric === 'recordsPerSecond' ? ' records/s' : '';
  return chartGroups(chart, dataset)
    .map((group) => ({ ...group, rows: group.rows.filter(isMeasured) }))
    .filter((group) => group.rows.length > 0)
    .map((group) => {
      const best = findClearBest(group.rows, rule);
      const text =
        best === null
          ? noClearBestText
          : `${clientLabel(best.client)} (${formatChartValue(best.median, chart.metric)}${unitSuffix})`;
      return chart.scenario === 'F1' ? text : `${group.label}: ${text}`;
    })
    .join(', ');
}

/** Kept under about 50 characters so it fits a 320 px wide chart; caveats live on the page. */
function chartSubtitle(chart, dataset) {
  const rows = chartGroups(chart, dataset).flatMap((group) => group.rows);
  const parts = [
    chart.scenario === 'F1'
      ? 'Draft. One measurement per client.'
      : `Draft. ${payloadLabel(dataset.payload)} values, median of ${dataset.sessions.competitors.repetitions} runs.`,
  ];
  if (rows.some((row) => isEarlierCoreBuild(row, dataset))) parts.push('kafka-core: earlier build.');
  return parts.join(' ');
}

/**
 * A round axis end about 30% past the largest bar, so its value label fits beside the bar even on a
 * 320 px wide chart instead of being drawn over it.
 */
function axisMaxWithLabelRoom(largestValue) {
  const target = largestValue * 1.3;
  const step = 10 ** Math.floor(Math.log10(target)) / 2;
  return Math.ceil(target / step) * step;
}

/** One ```chart block: a horizontal grouped bar chart, one series per client, one group per runtime. */
function chartBlock(chart, dataset) {
  const groups = chartGroups(chart, dataset);
  const { divisor = 1, decimals } = METRICS[chart.metric];
  // Values in the chart's display unit; MiB and MB need fractions that bytes did not.
  const valueDecimals = divisor === 1 ? decimals : 2;
  const series = CLIENTS.map((client) => ({
    name: client.label,
    data: groups.map((group) => {
      const row = group.rows.find((entry) => entry.client === client.id);
      if (row === undefined || !isMeasured(row)) return null;
      return round(row.median / divisor, valueDecimals);
    }),
  }));
  const bestLabel = chart.better === 'higher' ? 'Highest' : 'Lowest';
  const largestValue = Math.max(...series.flatMap((entry) => entry.data).filter((value) => value !== null));
  const subject =
    chart.scenario === 'F1'
      ? `${chart.name.toLowerCase()} by client`
      : `${chart.name.toLowerCase()} with ${payloadLabel(dataset.payload)} values by runtime and client`;
  const options = {
    description: `Bar chart of ${subject}, ${chart.better} is better. ${bestLabel}: ${bestPerGroupText(chart, dataset)}. The table below lists every value.`,
    chart: { type: 'bar', height: groups.length === 1 ? 240 : 420 },
    plotOptions: { bar: { horizontal: true, barHeight: '85%' } },
    title: { text: chartTitle(chart) },
    subtitle: { text: chartSubtitle(chart, dataset) },
    series,
    xaxis: {
      categories: groups.map((group) => (chart.scenario === 'F1' ? 'Install size' : group.label)),
      title: { text: METRICS[chart.metric].axisTitle },
      max: axisMaxWithLabelRoom(largestValue),
    },
  };
  return ['```chart', formatJson(options), '```'].join('\n');
}

/** Median and min–max cells of one table row for one payload. */
function resultCells(cell, metric) {
  if (cell === undefined || cell.status === 'not-measured') return ['not measured', '—'];
  if (cell.status === 'unsupported') return ['not supported', '—'];
  if (cell.status === 'failed') return [`all ${cell.runs} runs failed`, '—'];
  return [
    formatTableValue(cell.median, metric),
    `${formatTableValue(cell.min, metric)}–${formatTableValue(cell.max, metric)}`,
  ];
}

/** Every value of the chart, with one median and range column pair per payload that has results. */
function chartTable(chart, datasets) {
  const metric = METRICS[chart.metric];
  if (chart.scenario === 'F1') {
    const rows = datasets[0].footprints.map((footprint) => [
      clientLabel(footprint.client),
      footprint.status === 'ok' ? formatTableValue(footprint.installBytes, chart.metric) : 'not measured',
    ]);
    return markdownTable(['Client', `Install size (${metric.unit})`], rows);
  }
  const header = ['Runtime', 'Client'];
  for (const dataset of datasets) {
    const label = payloadLabel(dataset.payload);
    header.push(`${label} median (${metric.unit})`, `${label} min–max`);
  }
  const rows = RUNTIMES.flatMap((runtime) =>
    CLIENTS.map((client) => [
      runtimeLabel(runtime.id),
      client.label,
      ...datasets.flatMap((dataset) =>
        resultCells(
          chartData(chart, dataset).cells.find((cell) => cell.runtime === runtime.id && cell.client === client.id),
          chart.metric,
        ),
      ),
    ]),
  );
  return markdownTable(header, rows);
}

function chartSection(chart, comparison) {
  const datasets = orderedDatasets(comparison).filter((dataset) => hasResults(chart, dataset));
  if (datasets.length === 0) return ['Not measured in the published sessions yet.'];
  // Install size does not depend on the payload, so it shows one session's measurement.
  const shown = chart.scenario === 'F1' ? datasets.slice(0, 1) : datasets;
  return [chartBlock(chart, shown[0]), '', chartTable(chart, shown)];
}

function listText(items) {
  if (items.length <= 1) return items.join('');
  return `${items.slice(0, -1).join(', ')} and ${items.at(-1)}`;
}

function introText(comparison) {
  const competitorsText = listText(
    CLIENTS.filter((client) => client.id !== CORE_CLIENT_ID).map((client) => `\`${client.packageName}\``),
  );
  const runtimesText = listText(RUNTIMES.map((runtime) => runtime.label));
  const isEveryPayloadPublished = orderedDatasets(comparison).length === PAYLOADS.length;
  const lines = [
    `How \`@cookiemonsterdev/kafka-core\` compares with ${competitorsText} on ${runtimesText}. The headline workload sends ${HEADLINE_PAYLOAD.label} values with a key and headers. A second workload with ${STRESS_PAYLOAD.label} values is a stress test of per-record client overhead.${isEveryPayloadPublished ? ' Each table shows both side by side.' : ''}`,
  ];
  if (!isEveryPayloadPublished) {
    const missing = PAYLOADS.filter((payload) => comparison.datasets[payload.id] === undefined);
    const shown = orderedDatasets(comparison).map((dataset) => payloadLabel(dataset.payload));
    lines.push(
      '',
      `**${listText(missing.map((payload) => payload.label))} results are not published yet.** Until they are, the tables have only ${listText(shown)} columns, and every chart shows ${listText(shown)} values and says so in its subtitle.`,
    );
  }
  return lines;
}

const CAVEATS = [
  `The ${STRESS_PAYLOAD.label} results stress per-record client overhead: with little data per record, they rank clients mostly by the work each does per record. The ${HEADLINE_PAYLOAD.label} results are the headline.`,
  'Only kafka-core verifies record-batch CRCs while consuming, so it does extra work in the consume and latency results.',
  'Concurrent sends favour clients that merge separate `send()` calls into one batch, as kafka-core does. Batched send is the fairer produce comparison.',
  "Confluent's client runs on Node.js 24 only. Its consume runs can run out of memory under the 2 GiB limit: a median covers the runs that finished, and a cell where every run failed has no value.",
  'All results come from one machine. Treat small differences as inconclusive.',
];

/** One column per stored dataset, headline payload first. */
function payloadColumns(datasets) {
  return datasets.map((dataset) => payloadLabel(dataset.payload));
}

function payloadText() {
  const shapeRows = PAYLOADS.map((payload) => {
    const shape = payloadShapes[payload.id];
    const headers = shape.headerCount === 0 ? 'none' : `${shape.headerCount}, ${headerValueBytes} B values`;
    return [payload.label, payload.role, `${shape.keyBytes} B`, `${formatCount(shape.valueBytes)} B`, headers];
  });
  const examples = PAYLOADS.flatMap((payload, index) => {
    const [record] = createRecordPool(payload.id, payloadSeed, 1);
    const value = record.value.toString('latin1');
    const headers =
      record.headers === undefined
        ? 'none'
        : Object.entries(record.headers)
            .map(([name, headerValue]) => `${name}=${headerValue.toString('latin1')}`)
            .join(', ');
    return [
      ...(index === 0 ? [] : ['']),
      `${payload.label} payload`,
      `  key:     ${record.key.toString('latin1')}`,
      `  value:   ${value.slice(0, 32)}… (${formatCount(value.length)} bytes)`,
      `  headers: ${headers}`,
    ];
  });
  return [
    '### Payloads',
    '',
    markdownTable(['Payload', 'Role', 'Key', 'Value', 'Headers'], shapeRows),
    '',
    `Records come from a fixed seed, so every client sends the same bytes in every session. Produce scenarios send the key, value and headers. Consume topics are seeded by Kafka's own \`kafka-producer-perf-test.sh\` with values only. The first record of each pool:`,
    '',
    '```text',
    ...examples,
    '```',
  ];
}

function scenarioLabel(scenario) {
  return `${scenario} \`${scenarioDefinitions[scenario].name}\``;
}

/** Scenario ids any dataset ran, in catalog order. */
function workloadScenarios(datasets) {
  return scenarioIds.filter((scenario) =>
    datasets.some((dataset) => dataset.workload.some((entry) => entry.scenario === scenario)),
  );
}

function countCells(entry) {
  if (entry === undefined) return ['not run', '—', '—'];
  const { kind } = scenarioDefinitions[entry.scenario];
  const records =
    kind === 'e2e'
      ? `${formatCount(entry.rateMessagesPerSecond)}/s for ${trimZeros((entry.durationMs / 1000).toFixed(1))} s`
      : formatCount(entry.messageCount);
  const measured =
    entry.measuredSeconds === null
      ? '—'
      : `${entry.measuredSeconds.fastest.toFixed(1)}–${entry.measuredSeconds.slowest.toFixed(1)} s`;
  return [records, formatCount(entry.warmupCount), measured];
}

function recordCountsText(datasets) {
  const header = ['Scenario'];
  for (const label of payloadColumns(datasets)) {
    header.push(`${label} measured records`, `${label} warm-up`, `${label} measured time`);
  }
  const rows = workloadScenarios(datasets).map((scenario) => [
    scenarioLabel(scenario),
    ...datasets.flatMap((dataset) => countCells(dataset.workload.find((entry) => entry.scenario === scenario))),
  ]);
  const partitions = [...new Set(datasets.flatMap((dataset) => dataset.workload.map((entry) => entry.partitions)))];
  return [
    '### Record counts',
    '',
    `Each run sends or reads its warm-up records first and measures only the rest. Measured time is the median time of the fastest and of the slowest client and runtime under the normalized profile; E1 instead runs for its fixed duration, and L1 times one record from process start. Every run gets a fresh topic with ${listText(partitions.map(String))} partitions, deleted before the next run starts.`,
    '',
    markdownTable(header, rows),
  ];
}

/** The wire settings each scenario pinned; every dataset must agree, because the tables show them once. */
function scenarioWire(datasets, scenario) {
  const wires = datasets
    .map((dataset) => dataset.workload.find((entry) => entry.scenario === scenario)?.wire)
    .filter((wire) => wire !== undefined);
  if (new Set(wires.map((wire) => JSON.stringify(wire))).size > 1) {
    throw new Error(`The published datasets ran ${scenario} with different wire settings; re-measure them together`);
  }
  return wires[0];
}

function produceCalls(definition) {
  if (definition.kind === 'e2e') return 'one record per `send()` at a fixed rate';
  if (definition.kind === 'startup') return 'one record';
  const records = `${definition.recordsPerSend} record${definition.recordsPerSend === 1 ? '' : 's'}`;
  if (definition.concurrency === 1) return `sequential \`send()\` of ${records}`;
  return `up to ${definition.concurrency} concurrent \`send()\` calls of ${records}`;
}

function consumeHandler(definition) {
  const parts = [definition.consumeMode === 'each-batch' ? 'batch' : 'per record'];
  if (definition.commitEvery > 0) parts.push(`commit every ${formatCount(definition.commitEvery)} records`);
  if (definition.seedCompression !== 'none') parts.push(`${definition.seedCompression}-seeded topic`);
  return parts.join(', ');
}

function wireSettingsText(datasets) {
  const scenarioList = workloadScenarios(datasets);
  const ofKinds = (kinds) => scenarioList.filter((scenario) => kinds.includes(scenarioDefinitions[scenario].kind));
  const produceRows = ofKinds(['produce', 'e2e', 'startup']).map((scenario) => {
    const wire = scenarioWire(datasets, scenario);
    return [
      scenarioLabel(scenario),
      produceCalls(scenarioDefinitions[scenario]),
      wire.acks === -1 ? 'all' : String(wire.acks),
      `${wire.lingerMs} ms`,
      formatSettingBytes(wire.batchBytes),
      String(wire.maxInFlight),
      wire.compression,
      wire.idempotent ? 'yes' : 'no',
    ];
  });
  const consumeRows = ofKinds(['consume', 'e2e', 'startup']).map((scenario) => {
    const wire = scenarioWire(datasets, scenario);
    return [
      scenarioLabel(scenario),
      consumeHandler(scenarioDefinitions[scenario]),
      formatSettingBytes(wire.fetchMinBytes),
      formatSettingBytes(wire.fetchMaxBytes),
      formatSettingBytes(wire.partitionFetchMaxBytes),
      `${wire.fetchMaxWaitMs} ms`,
    ];
  });
  return [
    '### Wire settings',
    '',
    'The normalized profile pins these settings for every client; each adapter maps them to its own options. The library-defaults chart sets only brokers, client id and group id.',
    '',
    markdownTable(
      ['Scenario', 'Produce calls', 'Acks', 'Linger', 'Batch size', 'Max in-flight', 'Compression', 'Idempotent'],
      produceRows,
    ),
    '',
    markdownTable(
      ['Scenario', 'Consume handler', 'Fetch min', 'Fetch max', 'Partition fetch max', 'Fetch max wait'],
      consumeRows,
    ),
  ];
}

function sessionCellText(dataset) {
  const { competitors, core } = dataset.sessions;
  const text = `\`${competitors.sessionId}\`, ${competitors.tier} tier, count scale ${competitors.countScale}`;
  if (core.sessionId === competitors.sessionId) return text;
  return `${text}; kafka-core from \`${core.sessionId}\``;
}

function environmentText(datasets) {
  const rows = [
    ['Session', sessionCellText],
    ['Date', (dataset) => dataset.sessions.competitors.startedAt.slice(0, 10)],
    ['Repetitions', (dataset) => `${dataset.sessions.competitors.repetitions}, interleaved`],
    [
      'Clients',
      ({ sessions }) =>
        listText(
          CLIENTS.map((client) => {
            const session = client.id === CORE_CLIENT_ID ? sessions.core : sessions.competitors;
            const build = client.id === CORE_CLIENT_ID ? ` (\`${session.coreSha.slice(0, 12)}\`)` : '';
            return `${client.label} ${session.clientVersions[client.id]}${build}`;
          }),
        ),
    ],
    [
      'Runtimes',
      ({ sessions }) =>
        listText(
          RUNTIMES.map(
            (runtime) => `${runtime.label.split(' ')[0]} ${sessions.competitors.runtimeVersions[runtime.id]}`,
          ),
        ),
    ],
    [
      'Host',
      ({ environment }) =>
        `${environment.cpuModel}, ${environment.cpuCount} CPUs, ${formatBytesAsGiB(environment.memoryBytes)}, ${environment.platform} ${environment.osRelease} ${environment.arch}`,
    ],
    [
      'Docker VM',
      ({ environment }) =>
        `Docker ${environment.dockerServerVersion}, ${environment.dockerVmCpus} vCPUs, ${formatBytesAsGiB(environment.dockerVmMemoryBytes)}`,
    ],
    [
      'Broker',
      ({ environment }) =>
        `\`${environment.brokerImage}\`, one KRaft node; ${cpusetSize(environment.brokerCpuset)} vCPUs (cpuset \`${environment.brokerCpuset}\`), ${formatBytesAsGiB(environment.brokerMemoryBytes)} memory, ${formatBytesAsGiB(environment.brokerHeapBytes)} heap, ${formatBytesAsGiB(environment.brokerTmpfsBytes)} tmpfs log`,
    ],
    [
      'Client container',
      ({ environment }) =>
        `${cpusetSize(environment.clientCpuset)} vCPUs (cpuset \`${environment.clientCpuset}\`), ${formatBytesAsGiB(environment.clientMemoryBytes)} memory, swap off; a fresh container per run`,
    ],
  ];
  return [
    '### Environment',
    '',
    `Every container runs on one Docker VM, each pinned to its own CPUs. Scenarios, fairness rules and the option mapping of each client are in the [benchmarks README](${BENCHMARKS_README_URL}).`,
    '',
    markdownTable(
      ['Setting', ...payloadColumns(datasets)],
      rows.map(([label, cellText]) => [label, ...datasets.map(cellText)]),
    ),
  ];
}

function reproduceText(comparison) {
  const lines = [
    '## How to reproduce',
    '',
    'From the repository root, with Docker running, run one session per payload. Regenerating the page from a session replaces only the results of its payload. A seed repeats the run order of the published session; record counts come from the current runner, so they can differ from the published ones listed under [Record counts](#record-counts).',
  ];
  for (const payload of PAYLOADS) {
    const seed = comparison.datasets[payload.id]?.sessions.competitors.seed;
    lines.push(
      '',
      `### ${payload.label} values`,
      '',
      '```sh',
      `pnpm benchmarks:run --tier standard --payload ${payload.id}${seed === undefined ? '' : ` --seed ${seed}`}`,
      'pnpm docs:benchmarks --competitors benchmarks/results/raw/<session> --core benchmarks/results/raw/<session>',
      '```',
    );
  }
  lines.push(
    '',
    '### Results from other hardware',
    '',
    `Results hold as ratios on the machine that produced them. To contribute results from another machine, run \`pnpm benchmarks:doctor\` and fix what it reports, run both sessions above, then [open an issue](${NEW_ISSUE_URL}) with each session's \`summary.json\` and \`report.md\` from \`benchmarks/results/raw/<session>/\`. Do not edit this page: it is generated.`,
    '',
  );
  return lines;
}

function renderPage(comparison) {
  const datasets = orderedDatasets(comparison);
  const lines = [
    '---',
    'title: Benchmarks',
    "description: Draft comparison of kafka-core with @platformatic/kafka, KafkaJS and Confluent's client on Node.js and Bun",
    'order: 12',
    'section: reference',
    '---',
    '',
    '<!-- Generated by scripts/generate-benchmark-docs.mjs. Edit the script, not this file. -->',
    '',
    ...introText(comparison),
    '',
    '## Read this first',
    '',
    ...CAVEATS.map((caveat) => `- ${caveat}`),
    '',
    '## Setup',
    '',
    ...payloadText(),
    '',
    ...recordCountsText(datasets),
    '',
    ...wireSettingsText(datasets),
    '',
    ...environmentText(datasets),
  ];
  let currentSection = null;
  for (const chart of CHARTS) {
    if (chart.section !== currentSection) {
      currentSection = chart.section;
      lines.push('', `## ${chart.section}`);
    }
    if (chart.heading !== undefined) lines.push('', `### ${chart.heading}`);
    lines.push('', chart.description, '', ...chartSection(chart, comparison));
  }
  lines.push('', ...reproduceText(comparison));
  return lines.join('\n');
}

// ---------------------------------------------------------------------------------------------------

const PRINT_WIDTH = 120;

/**
 * `JSON.stringify(value, null, 2)`, except that an array of primitives stays on one line when it fits
 * within the repository's Prettier print width, as Prettier prints it. Keeps the file stable under
 * `pnpm format:check` without depending on Prettier here.
 */
function formatJson(value, indent = '', linePrefixLength = 0, suffixLength = 0) {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  const innerIndent = `${indent}  `;
  if (Array.isArray(value)) {
    if (value.length === 0) return '[]';
    const isPrimitiveArray = value.every((item) => item === null || typeof item !== 'object');
    const inline = `[${value.map((item) => JSON.stringify(item)).join(', ')}]`;
    if (isPrimitiveArray && linePrefixLength + inline.length + suffixLength <= PRINT_WIDTH) return inline;
    const items = value.map((item, index) => {
      const comma = index < value.length - 1 ? 1 : 0;
      return `${innerIndent}${formatJson(item, innerIndent, innerIndent.length, comma)}`;
    });
    return `[\n${items.join(',\n')}\n${indent}]`;
  }
  const entries = Object.entries(value);
  if (entries.length === 0) return '{}';
  const lines = entries.map(([key, entry], index) => {
    const prefix = `${innerIndent}${JSON.stringify(key)}: `;
    const comma = index < entries.length - 1 ? 1 : 0;
    return `${prefix}${formatJson(entry, innerIndent, prefix.length, comma)}`;
  });
  return `{\n${lines.join(',\n')}\n${indent}}`;
}

function parseOptions() {
  const { values } = parseArgs({
    options: {
      competitors: { type: 'string' },
      core: { type: 'string' },
      'core-fallback': { type: 'string' },
      check: { type: 'boolean', default: false },
    },
  });
  const hasSessions = values.competitors !== undefined || values.core !== undefined;
  if (hasSessions && (values.competitors === undefined || values.core === undefined)) {
    throw new Error('Pass both --competitors and --core, or neither to re-render from the committed JSON.');
  }
  return values;
}

/** The committed datasets, or none when the JSON does not exist yet. */
function readCommittedDatasets() {
  if (!existsSync(DATA_PATH)) return {};
  const committed = readJson(DATA_PATH);
  if (committed.schemaVersion !== SCHEMA_VERSION) {
    throw new Error(`${DATA_PATH} has schemaVersion ${committed.schemaVersion}; this script reads ${SCHEMA_VERSION}.`);
  }
  return committed.datasets;
}

/** Every output file and its expected content. */
function expectedOutputs(options) {
  const datasets = readCommittedDatasets();
  if (options.competitors !== undefined) {
    // The same directory is often passed twice; each session reads hundreds of run files.
    const sessionsByPath = new Map();
    const load = (directory) => {
      const path = resolve(directory);
      if (!sessionsByPath.has(path)) sessionsByPath.set(path, loadSession(path));
      return sessionsByPath.get(path);
    };
    const dataset = buildDataset({
      competitors: load(options.competitors),
      core: load(options.core),
      coreFallback: load(options['core-fallback'] ?? options.competitors),
    });
    datasets[dataset.payload] = dataset;
  }
  if (Object.keys(datasets).length === 0) {
    throw new Error(`${DATA_PATH} holds no results; pass --competitors and --core session directories.`);
  }
  const comparison = {
    schemaVersion: SCHEMA_VERSION,
    status: 'draft',
    generator: 'scripts/generate-benchmark-docs.mjs',
    datasets: Object.fromEntries(
      PAYLOADS.filter((payload) => datasets[payload.id] !== undefined).map((payload) => [
        payload.id,
        datasets[payload.id],
      ]),
    ),
  };
  return new Map([
    [DATA_PATH, `${formatJson(comparison)}\n`],
    [PAGE_PATH, renderPage(comparison)],
  ]);
}

function main() {
  const options = parseOptions();
  const outputs = expectedOutputs(options);
  if (options.check) {
    const outdated = [...outputs].filter(
      ([path, content]) => !existsSync(path) || readFileSync(path, 'utf8') !== content,
    );
    for (const [path] of outdated) console.error(`out of date: ${path}`);
    if (outdated.length > 0) {
      console.error('Run node scripts/generate-benchmark-docs.mjs to regenerate.');
      process.exitCode = 1;
    }
    return;
  }
  mkdirSync(dirname(DATA_PATH), { recursive: true });
  for (const [path, content] of outputs) writeFileSync(path, content);
  console.log(`wrote ${outputs.size} files`);
}

main();
