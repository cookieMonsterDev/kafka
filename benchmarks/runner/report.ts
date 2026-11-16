import { writeFile } from 'node:fs/promises';

import { clientIds, scenarioIds } from '../client/spec.ts';
import type { ScenarioId } from '../client/spec.ts';
import { aggregateSession, baselineSeries } from './aggregate.ts';
import type { CellSummary, ComparedMetric, Comparison, SessionSummary, StatSummary } from './aggregate.ts';
import { sessionPaths } from './session.ts';
import type { Footprint, FootprintPart } from './types.ts';

function formatNumber(value: number, fractionDigits = 0): string {
  return value.toLocaleString('en-US', {
    minimumFractionDigits: fractionDigits,
    maximumFractionDigits: fractionDigits,
  });
}

function formatStat(stat: StatSummary | null, fractionDigits = 0): string {
  if (!stat) {
    return 'n/a';
  }
  const median = formatNumber(stat.median, fractionDigits);
  if (stat.samples === 1) {
    return median;
  }
  return `${median} (${formatNumber(stat.min, fractionDigits)}–${formatNumber(stat.max, fractionDigits)})`;
}

function formatMebibytes(stat: StatSummary | null): string {
  if (!stat) {
    return 'n/a';
  }
  return formatNumber(stat.median / 1_048_576, 0);
}

/**
 * Where a client's closure holds a native addon: `yes` when it is required, `optional only`, or `no`. A
 * session recorded before the split knows only whether the whole closure holds one: `yes, not split`.
 */
export function formatNativeAddon(footprint: Footprint): string {
  if (footprint.unsplitTotal !== undefined) {
    return footprint.unsplitTotal.hasNativeAddon ? 'yes, not split' : 'no';
  }
  if (footprint.required === null || footprint.optional === null) {
    return 'n/a';
  }
  if (footprint.required.hasNativeAddon) {
    return 'yes';
  }
  return footprint.optional.hasNativeAddon ? 'optional only' : 'no';
}

/**
 * Bytes and dependencies of the whole closure: required plus optional, or the total of a session recorded
 * before the split. Null when the package is not installed.
 */
export function footprintTotal(footprint: Footprint): { bytes: number; dependencyCount: number } | null {
  if (footprint.unsplitTotal !== undefined) {
    return footprint.unsplitTotal;
  }
  const { required, optional } = footprint;
  if (required === null || optional === null) {
    return null;
  }
  return {
    bytes: required.bytes + optional.bytes,
    dependencyCount: required.dependencyCount + optional.dependencyCount,
  };
}

/** Whether any footprint comes from a session recorded before the split, so its table needs total columns. */
export function hasUnsplitFootprint(footprints: readonly Footprint[]): boolean {
  return footprints.some((footprint) => footprint.unsplitTotal !== undefined);
}

/** `missingText` fills the cells of a part that is null. */
function footprintPartCells(part: FootprintPart | null, missingText: string): string[] {
  if (part === null) {
    return [missingText, missingText, missingText];
  }
  return [formatNumber(part.bytes / 1_048_576, 1), String(part.dependencyCount), formatNumber(part.fileCount)];
}

function footprintTotalCells(footprint: Footprint): string[] {
  const total = footprintTotal(footprint);
  if (total === null) {
    return ['n/a', 'n/a'];
  }
  return [formatNumber(total.bytes / 1_048_576, 1), String(total.dependencyCount)];
}

function formatPercent(fraction: number): string {
  return `${formatNumber(fraction * 100, 1)}%`;
}

const metricLabels: Readonly<Record<ComparedMetric, string>> = {
  recordsPerSecond: 'records/s',
  latencyP50Ms: 'p50',
  latencyP99Ms: 'p99',
  firstAckedProduceMs: 'first ack',
};

function formatComparison(comparison: Comparison): string {
  return `${formatNumber(comparison.ratio, 2)}× (${comparison.verdict})`;
}

/** `1.05× (better)` for one comparison; with several (E1's p99 and p50), each prefixed with its metric. */
function formatComparisons(comparisons: readonly Comparison[]): string {
  if (comparisons.length === 1) {
    return formatComparison(comparisons[0]!);
  }
  return comparisons
    .map((comparison) => `${metricLabels[comparison.metric]} ${formatComparison(comparison)}`)
    .join('; ');
}

function table(header: readonly string[], rows: readonly (readonly string[])[]): string {
  const escape = (cell: string) => cell.replaceAll('|', '\\|');
  const lines = [
    `| ${header.map(escape).join(' | ')} |`,
    `| ${header.map(() => '---').join(' | ')} |`,
    ...rows.map((row) => `| ${row.map(escape).join(' | ')} |`),
  ];
  return lines.join('\n');
}

function formatNoiseFloors(summary: SessionSummary): string {
  const parts: string[] = [];
  if (summary.noiseFloor !== null) {
    parts.push(`${formatPercent(summary.noiseFloor)} from calibration`);
  }
  const sessionFloors = Object.entries(summary.aaNoiseFloors).flatMap(([scenario, floors]) =>
    Object.entries(floors).map(
      ([metric, floor]) => `${scenario} ${metricLabels[metric as ComparedMetric]} ${formatPercent(floor)}`,
    ),
  );
  if (sessionFloors.length > 0) {
    parts.push(`A/A in this session: ${sessionFloors.join(', ')}`);
  }
  if (parts.length === 0) {
    return 'none given; only non-overlapping ranges decide a comparison';
  }
  return parts.join('; ');
}

function renderHeader(summary: SessionSummary): string {
  const { manifest, environment } = summary;
  const lines = [
    `# Benchmark session ${summary.sessionId}`,
    '',
    `- Tier: \`${manifest.tier}\`, repetitions: ${manifest.repetitions}, shuffle seed: ${manifest.seed}`,
    `- Payload: \`${manifest.payload}\`, count scale: ${manifest.countScale}`,
    `- kafka-core: \`${manifest.core.sha}\` (${manifest.core.requestedRef}, committed ${manifest.core.commitDate})`,
    `- Broker: \`${manifest.broker.image}\`${manifest.broker.digest ? ` (\`${manifest.broker.digest}\`)` : ''}`,
    `- Started ${manifest.startedAt}, finished ${manifest.finishedAt ?? 'not finished'}`,
  ];
  if (environment) {
    const { host, docker, budget } = environment;
    lines.push(
      `- Host: ${host.cpuModel}, ${host.cpuCount} CPUs, ${formatNumber(host.memoryBytes / 1_073_741_824, 0)} GiB, ${host.platform} ${host.osRelease}`,
      `- Docker ${docker.serverVersion}: ${docker.vmCpus} vCPUs, ${formatNumber(docker.vmMemoryBytes / 1_073_741_824, 1)} GiB; broker cpuset \`${budget.brokerCpuset}\`, client cpuset \`${budget.clientCpuset}\``,
    );
    if (environment.warnings.length > 0) {
      lines.push(`- **Doctor warnings** (not publishable): ${environment.warnings.join(', ')}`);
    }
  }
  lines.push(
    `- Noise threshold: CV above ${formatPercent(summary.noiseThresholdCv)} marks a cell noisy`,
    `- Noise floor: ${formatNoiseFloors(summary)}`,
  );
  return lines.join('\n');
}

function renderRuntimes(summary: SessionSummary): string {
  const rows = summary.manifest.images.map((image) => [
    image.runtime,
    image.runtimeVersion,
    `\`${image.baseImage}\``,
    ...clientIds.map((client) => image.clientVersions[client] ?? 'not installed'),
  ]);
  return ['## Runtimes and versions', '', table(['Runtime', 'Version', 'Base image', ...clientIds], rows)].join('\n');
}

function renderSupportMatrix(summary: SessionSummary): string {
  if (!summary.supportMatrix) {
    return '';
  }
  const rows = summary.supportMatrix.cells.map((cell) => [
    cell.client,
    cell.runtime,
    cell.supported ? 'supported' : 'unsupported',
    cell.reason,
  ]);
  return ['## Support matrix', '', table(['Client', 'Runtime', 'Status', 'Reason'], rows)].join('\n');
}

const concurrentSendBias =
  'Structurally biased: 256 workers each await their own single-record `send()`, so this measures whether and when a ' +
  'client merges records from separate calls, not protocol speed. kafkajs and Platformatic send one request per ' +
  'call, librdkafka flushes each partition batch at `batch.size` or after `linger.ms`, and kafka-core flushes once ' +
  'its buffered bytes across all partitions reach `batchSize`. Use P2 as the headline produce comparison.';

/** Reading notes printed under a scenario heading, for results that need context to compare fairly. */
const scenarioNotes: Partial<Record<ScenarioId, string>> = {
  P2: 'Headline produce comparison: every client sends the same 100-record batches with the same wire settings.',
  P3: concurrentSendBias,
  P4: concurrentSendBias,
  P5: concurrentSendBias,
};

function renderScenario(summary: SessionSummary, scenario: ScenarioId, footnotes: Map<string, number>): string {
  const cells = summary.cells.filter((cell) => cell.scenario === scenario);
  const first = cells[0];
  if (!first) {
    return '';
  }
  const comparisonsFor = (cell: CellSummary) =>
    summary.comparisons.filter(
      (comparison) =>
        comparison.scenario === cell.scenario &&
        comparison.runtime === cell.runtime &&
        comparison.profile === cell.profile &&
        comparison.series === cell.series,
    );
  const footnoteMarks = (cell: CellSummary) =>
    cell.capabilityGaps
      .map((gap) => {
        const existing = footnotes.get(gap);
        if (existing !== undefined) {
          return existing;
        }
        const next = footnotes.size + 1;
        footnotes.set(gap, next);
        return next;
      })
      .map((mark) => `[^${mark}]`)
      .join('');

  const isStartup = first.kind === 'startup';
  const header = isStartup
    ? ['Runtime', 'Profile', 'Series', 'First ack ms', 'First consume ms', 'Peak MiB', 'Runs', `vs ${baselineSeries}`]
    : [
        'Runtime',
        'Profile',
        'Series',
        'Records/s',
        'MB/s',
        'p50 ms',
        'p99 ms',
        'CPU s',
        'Records/CPU s',
        'Peak MiB',
        'CV',
        'Runs',
        `vs ${baselineSeries}`,
      ];
  const rows = cells.map((cell) => {
    const series = `${cell.series}${cell.noisy ? ' (noisy)' : ''}${footnoteMarks(cell)}`;
    const runs = `${cell.passedRuns}/${cell.runs}`;
    const versus = formatComparisons(comparisonsFor(cell));
    const { metrics } = cell;
    if (isStartup) {
      return [
        cell.runtime,
        cell.profile,
        series,
        formatStat(metrics.firstAckedProduceMs, 1),
        formatStat(metrics.firstConsumedRecordMs, 1),
        formatMebibytes(metrics.peakMemoryBytes),
        runs,
        versus,
      ];
    }
    return [
      cell.runtime,
      cell.profile,
      series,
      formatStat(metrics.recordsPerSecond),
      formatStat(metrics.megabytesPerSecond, 1),
      formatStat(metrics.latencyP50Ms, 2),
      formatStat(metrics.latencyP99Ms, 2),
      formatStat(metrics.cpuSeconds, 2),
      formatStat(metrics.recordsPerCpuSecond),
      formatMebibytes(metrics.peakMemoryBytes),
      formatPercent(metrics[cell.primaryMetric]?.cv ?? 0),
      runs,
      versus,
    ];
  });
  const note = scenarioNotes[scenario];
  const heading = `### ${first.scenario} \`${first.scenarioName}\``;
  return [heading, '', ...(note === undefined ? [] : [note, '']), table(header, rows)].join('\n');
}

function renderFootprints(summary: SessionSummary): string {
  if (!summary.footprints || summary.footprints.length === 0) {
    return '';
  }
  const hasTotals = hasUnsplitFootprint(summary.footprints);
  const rows = summary.footprints.map((footprint) => {
    const missingPartText = footprint.unsplitTotal === undefined ? 'n/a' : 'not split';
    return [
      footprint.packageName,
      footprint.version ?? 'n/a',
      ...(hasTotals ? footprintTotalCells(footprint) : []),
      ...footprintPartCells(footprint.required, missingPartText),
      ...footprintPartCells(footprint.optional, missingPartText),
      formatNativeAddon(footprint),
    ];
  });
  const header = [
    'Package',
    'Version',
    ...(hasTotals ? ['Total MiB', 'Total dependencies'] : []),
    'Required MiB',
    'Required dependencies',
    'Required files',
    'Optional MiB',
    'Optional dependencies',
    'Optional files',
    'Native addon',
  ];
  return [
    '## F1 `footprint`',
    '',
    'Information, not a comparison: what installing each client adds. Required is the package and what it reaches',
    'through `dependencies`; optional is what installed `optionalDependencies` add on top.',
    ...(hasTotals
      ? ['This session was recorded before F1 split the two, so it has only totals, and no file counts.']
      : []),
    '',
    table(header, rows),
  ].join('\n');
}

function renderFailures(summary: SessionSummary): string {
  if (summary.failures.length === 0) {
    return '';
  }
  const rows = summary.failures.map((failure) => [
    failure.runId,
    failure.status,
    failure.reason.split('\n')[0]?.slice(0, 160) ?? '',
  ]);
  return ['## Failed or unverified runs', '', table(['Run', 'Status', 'Reason'], rows)].join('\n');
}

/** Renders a session summary as Markdown. Ratios are the series median over the kafka-core median. */
export function renderReport(summary: SessionSummary): string {
  const footnotes = new Map<string, number>();
  const scenarioSections = scenarioIds
    .map((scenario) => renderScenario(summary, scenario, footnotes))
    .filter((section) => section !== '');
  const footnoteLines = [...footnotes].map(([gap, mark]) => `[^${mark}]: ${gap}`);
  const sections = [
    renderHeader(summary),
    renderRuntimes(summary),
    renderSupportMatrix(summary),
    [
      '## Results',
      '',
      'Medians with min–max over passed runs. MB is 10^6 bytes, MiB is 2^20 bytes. Throughput rows compare',
      'records/s, E1 compares p99 and p50 latency, L1 compares time to the first acknowledged produce. The CV',
      'column and the noisy mark use the first of these (p99 for E1).',
      '',
      scenarioSections.join('\n\n'),
    ].join('\n'),
    renderFootprints(summary),
    renderFailures(summary),
    footnoteLines.join('\n'),
  ];
  return `${sections.filter((section) => section !== '').join('\n\n')}\n`;
}

/** Writes `report.md` into the session directory, aggregating first when no summary is passed. */
export async function writeReport(sessionDir: string, summary?: SessionSummary): Promise<string> {
  const reportPath = sessionPaths(sessionDir).report;
  await writeFile(reportPath, renderReport(summary ?? (await aggregateSession(sessionDir))));
  return reportPath;
}
