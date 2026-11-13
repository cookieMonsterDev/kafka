#!/usr/bin/env node
// Generates the docs site's client benchmark page from saved benchmark sessions
// (`benchmarks/results/raw/<session>/`) in three steps that always run together:
//
//   1. With session directories: writes `packages/docs/src/data/benchmarks/comparison.json`, the numbers
//      the page shows plus the provenance of each one (session, client and runtime version, kafka-core
//      SHA). Without them, the committed JSON is reused, so a layout change needs no session data.
//   2. Writes the whole page, `packages/docs/src/content/docs/core/reference/benchmarks.md`: one ```chart
//      block (ApexCharts options as JSON, rendered by `packages/docs/src/lib/rehype-chart.ts`) and one
//      table per entry in `CHARTS`. Change the page here, not in the Markdown file.
//
// Usage:
//   node scripts/generate-benchmark-docs.mjs --competitors <session dir> --core <session dir>
//        [--core-fallback <session dir>]
//   node scripts/generate-benchmark-docs.mjs            # re-render from the committed JSON
//   node scripts/generate-benchmark-docs.mjs --check    # fail if any output is out of date
//
// Competitor cells come from `--competitors`. kafka-core cells come from `--core`, and cells that session
// did not measure come from `--core-fallback` (default: the `--competitors` session, which also measures
// kafka-core). Pass the same directory to both for a same-session run. Output is deterministic: no
// timestamps beyond the sessions' own, fixed ordering and fixed decimals.
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
const DATA_PATH = join(ROOT, 'packages/docs/src/data/benchmarks/comparison.json');
const PAGE_PATH = join(ROOT, 'packages/docs/src/content/docs/core/reference/benchmarks.md');
const BENCHMARKS_README_URL = 'https://github.com/cookieMonsterDev/kafka/blob/develop/benchmarks/README.md';

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
  const runtimeVersions = Object.fromEntries(
    summary.manifest.images.map((image) => [image.runtime, image.runtimeVersion]),
  );
  return { summary, runtimeVersions };
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

/** One chart cell with its provenance, or an explicit unsupported / not-measured marker. */
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

function buildComparison(sources) {
  const { environment, manifest } = sources.competitors.summary;
  const charts = CHARTS.filter((chart) => chart.scenario !== 'F1').map((chart) => ({
    id: chart.id,
    scenario: chart.scenario,
    profile: chart.profile,
    metric: chart.metric,
    unit: METRICS[chart.metric].unit,
    better: chart.better,
    declaredGaps: declaredGaps(chart, sources),
    cells: RUNTIMES.flatMap((runtime) =>
      CLIENTS.map((client) => buildCell({ chart, runtime: runtime.id, client: client.id, sources })),
    ),
  }));
  return {
    schemaVersion: 1,
    status: 'draft',
    generator: 'scripts/generate-benchmark-docs.mjs',
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
      clientCpuset: environment.budget.clientCpuset,
      clientMemoryBytes: environment.budget.clientMemoryBytes,
      brokerImage: manifest.broker.image,
    },
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

function formatBytesAsGiB(bytes) {
  return `${trimZeros((bytes / 1024 ** 3).toFixed(1))} GiB`;
}

function isMeasured(row) {
  return row.status === 'ok' || row.status === 'partial';
}

/** kafka-core cells taken from an older build than the main kafka-core session. */
function isEarlierCoreBuild(row, comparison) {
  return (
    row.client === CORE_CLIENT_ID &&
    row.source === 'core-fallback' &&
    row.coreSha !== undefined &&
    row.coreSha !== comparison.sessions.core.coreSha
  );
}

/** Short enough for a 320 px wide chart; the page heading above names the chart. */
function chartTitle(chart) {
  const direction = chart.better === 'higher' ? 'Higher' : 'Lower';
  return `${direction} is better (${METRICS[chart.metric].unit})`;
}

// ---------------------------------------------------------------------------------------------------
// Step 2: comparison.json → the Markdown page

/** Rows of one chart, grouped by runtime (or one group for install size). */
function chartGroups(chart, comparison) {
  if (chart.scenario === 'F1') {
    return [
      {
        label: 'Package and production dependencies',
        rows: comparison.footprints.map((footprint) => ({
          ...footprint,
          median: footprint.installBytes,
          min: footprint.installBytes,
          max: footprint.installBytes,
        })),
      },
    ];
  }
  const data = comparison.charts.find((entry) => entry.id === chart.id);
  return RUNTIMES.map((runtime) => ({
    label: runtimeLabel(runtime.id),
    rows: data.cells.filter((cell) => cell.runtime === runtime.id),
  }));
}

function markdownTable(header, rows) {
  const widths = header.map((cell, column) => Math.max(cell.length, ...rows.map((row) => row[column].length)));
  const line = (cells) => `| ${cells.map((cell, column) => cell.padEnd(widths[column])).join(' | ')} |`;
  return [line(header), `| ${widths.map((width) => '-'.repeat(width)).join(' | ')} |`, ...rows.map(line)].join('\n');
}

function bestPerGroupText(chart, comparison) {
  const isBetter = (a, b) => (chart.better === 'higher' ? a > b : a < b);
  return chartGroups(chart, comparison)
    .map((group) => {
      const best = group.rows
        .filter(isMeasured)
        .reduce((winner, row) => (isBetter(row.median, winner.median) ? row : winner));
      const unitSuffix = chart.metric === 'recordsPerSecond' ? ' records/s' : '';
      const value = `${formatChartValue(best.median, chart.metric)}${unitSuffix}`;
      if (chart.scenario === 'F1') return `${clientLabel(best.client)} (${value})`;
      return `${group.label}: ${clientLabel(best.client)} (${value})`;
    })
    .join(', ');
}

/** Kept under about 50 characters so it fits a 320 px wide chart; caveats live on the page. */
function chartSubtitle(chart, comparison) {
  const rows = chartGroups(chart, comparison).flatMap((group) => group.rows);
  const parts = [
    chart.scenario === 'F1'
      ? 'Draft. One measurement per client.'
      : `Draft. Median of ${comparison.sessions.competitors.repetitions} runs.`,
  ];
  if (rows.some((row) => isEarlierCoreBuild(row, comparison))) parts.push('kafka-core: earlier build.');
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
function chartBlock(chart, comparison) {
  const groups = chartGroups(chart, comparison);
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
  const options = {
    description: `Bar chart of ${chart.name.toLowerCase()} by ${chart.scenario === 'F1' ? 'client' : 'runtime and client'}, ${chart.better} is better. ${bestLabel}: ${bestPerGroupText(chart, comparison)}. The table below lists every value.`,
    chart: { type: 'bar', height: groups.length === 1 ? 240 : 420 },
    plotOptions: { bar: { horizontal: true, barHeight: '85%' } },
    title: { text: chartTitle(chart) },
    subtitle: { text: chartSubtitle(chart, comparison) },
    series,
    xaxis: {
      categories: groups.map((group) => (chart.scenario === 'F1' ? 'Install size' : group.label)),
      title: { text: METRICS[chart.metric].axisTitle },
      max: axisMaxWithLabelRoom(largestValue),
    },
  };
  return ['```chart', formatJson(options), '```'].join('\n');
}

function chartTable(chart, comparison) {
  const metric = METRICS[chart.metric];
  if (chart.scenario === 'F1') {
    const rows = comparison.footprints.map((footprint) => [
      clientLabel(footprint.client),
      footprint.status === 'ok' ? formatTableValue(footprint.installBytes, chart.metric) : 'not measured',
    ]);
    return markdownTable(['Client', `Install size (${metric.unit})`], rows);
  }
  const data = comparison.charts.find((entry) => entry.id === chart.id);
  const rows = data.cells.map((cell) => {
    const runtime = runtimeLabel(cell.runtime);
    const client = clientLabel(cell.client);
    if (cell.status === 'unsupported') return [runtime, client, 'not supported', '—'];
    if (cell.status === 'not-measured') return [runtime, client, 'not measured', '—'];
    const range = `${formatTableValue(cell.min, chart.metric)}–${formatTableValue(cell.max, chart.metric)}`;
    return [runtime, client, formatTableValue(cell.median, chart.metric), range];
  });
  return markdownTable(['Runtime', 'Client', `Median (${metric.unit})`, 'Min–max'], rows);
}

function listText(items) {
  if (items.length <= 1) return items.join('');
  return `${items.slice(0, -1).join(', ')} and ${items.at(-1)}`;
}

function introText(comparison) {
  const { competitors } = comparison.sessions;
  const competitorsText = listText(
    CLIENTS.filter((client) => client.id !== CORE_CLIENT_ID).map(
      (client) => `\`${client.packageName}\` ${competitors.clientVersions[client.id]}`,
    ),
  );
  const runtimesText = listText(
    RUNTIMES.map((runtime) => `${runtime.label.split(' ')[0]} ${competitors.runtimeVersions[runtime.id]}`),
  );
  return `How \`@cookiemonsterdev/kafka-core\` compares with ${competitorsText} on ${runtimesText}.`;
}

function methodText(comparison) {
  const { environment } = comparison;
  const { repetitions } = comparison.sessions.competitors;
  const clientCpus = environment.clientCpuset.split('-').map(Number);
  const clientCpuCount = clientCpus.length === 2 ? clientCpus[1] - clientCpus[0] + 1 : 1;
  return [
    `Each client ran in a fresh Docker container with ${clientCpuCount} vCPUs and ${formatBytesAsGiB(environment.clientMemoryBytes)}, against a single \`${environment.brokerImage}\` broker in KRaft mode, on one ${environment.cpuModel} laptop through Docker Desktop.`,
    `Every value is the median of ${repetitions} runs, with the min–max range. Scenarios, settings and fairness rules are in the [benchmarks README](${BENCHMARKS_README_URL}).`,
  ].join(' ');
}

const CAVEATS = [
  'Only kafka-core verifies record-batch CRCs while consuming, so it does extra work in the consume and latency results.',
  'Concurrent sends favour clients that merge separate `send()` calls into one batch, as kafka-core does. Batched send is the fairer produce comparison.',
  "Confluent's client runs on Node.js 24 only. Some of its consume runs ran out of memory, so those medians cover the remaining runs.",
  'All results come from one machine. Treat small differences as inconclusive.',
];

function renderPage(comparison) {
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
    introText(comparison),
    '',
    '## How it was measured',
    '',
    methodText(comparison),
    '',
    '## Read this first',
    '',
    ...CAVEATS.map((caveat) => `- ${caveat}`),
  ];
  let currentSection = null;
  for (const chart of CHARTS) {
    if (chart.section !== currentSection) {
      currentSection = chart.section;
      lines.push('', `## ${chart.section}`);
    }
    if (chart.heading !== undefined) lines.push('', `### ${chart.heading}`);
    lines.push('', chart.description, '', chartBlock(chart, comparison), '', chartTable(chart, comparison));
  }
  lines.push(
    '',
    '## How to reproduce',
    '',
    'From the repository root, with Docker running:',
    '',
    '```sh',
    `pnpm benchmarks:run --tier standard --seed ${comparison.sessions.competitors.seed}`,
    'pnpm docs:benchmarks --competitors benchmarks/results/raw/<session> --core benchmarks/results/raw/<session>',
    '```',
    '',
    'The first command measures every client and writes a session directory; the second regenerates this page and its charts from it.',
    '',
  );
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

/** Every output file and its expected content. */
function expectedOutputs(options) {
  const outputs = new Map();
  let comparison;
  if (options.competitors !== undefined) {
    const competitors = loadSession(options.competitors);
    const sources = {
      competitors,
      core: loadSession(options.core),
      coreFallback: options['core-fallback'] === undefined ? competitors : loadSession(options['core-fallback']),
    };
    comparison = buildComparison(sources);
    outputs.set(DATA_PATH, `${formatJson(comparison)}\n`);
  } else {
    comparison = readJson(DATA_PATH);
  }
  outputs.set(PAGE_PATH, renderPage(comparison));
  return outputs;
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
