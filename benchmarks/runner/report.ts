import { writeFile } from 'node:fs/promises';

import { clientIds, scenarioIds } from '../client/spec.ts';
import { aggregateSession, baselineSeries } from './aggregate.ts';
import type { CellSummary, Comparison, SessionSummary, StatSummary } from './aggregate.ts';
import { sessionPaths } from './session.ts';

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

function formatOptionalFlag(flag: boolean | null): string {
  if (flag === null) {
    return 'n/a';
  }
  return flag ? 'yes' : 'no';
}

function formatPercent(fraction: number): string {
  return `${formatNumber(fraction * 100, 1)}%`;
}

function formatComparison(comparison: Comparison | undefined): string {
  if (!comparison) {
    return '';
  }
  const ratio = `${formatNumber(comparison.ratio, 2)}×`;
  if (comparison.verdict === 'inconclusive') {
    return `${ratio} (inconclusive)`;
  }
  return `${ratio} (${comparison.verdict})`;
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
  const sessionFloors = Object.entries(summary.aaNoiseFloors).map(
    ([scenario, floor]) => `${scenario} ${formatPercent(floor)}`,
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

function renderScenario(summary: SessionSummary, scenario: string, footnotes: Map<string, number>): string {
  const cells = summary.cells.filter((cell) => cell.scenario === scenario);
  const first = cells[0];
  if (!first) {
    return '';
  }
  const comparisonFor = (cell: CellSummary) =>
    summary.comparisons.find(
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
    const versus = formatComparison(comparisonFor(cell));
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
  return [`### ${first.scenario} \`${first.scenarioName}\``, '', table(header, rows)].join('\n');
}

function renderFootprints(summary: SessionSummary): string {
  if (!summary.footprints || summary.footprints.length === 0) {
    return '';
  }
  const rows = summary.footprints.map((footprint) => [
    footprint.packageName,
    footprint.version ?? 'n/a',
    footprint.installBytes === null ? 'n/a' : formatNumber(footprint.installBytes / 1_048_576, 1),
    footprint.dependencyCount === null ? 'n/a' : String(footprint.dependencyCount),
    formatOptionalFlag(footprint.hasNativeAddon),
  ]);
  return [
    '## F1 `footprint`',
    '',
    table(['Package', 'Version', 'Install MiB', 'Dependencies', 'Native addon'], rows),
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
      'records/s, E1 compares p99 latency, L1 compares time to the first acknowledged produce.',
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
