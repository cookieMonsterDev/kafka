import { readdir } from 'node:fs/promises';
import { join } from 'node:path';

import { scenarios } from '../client/spec.ts';
import type { ClientId, Profile, RunRecord, RuntimeId, ScenarioId, ScenarioKind } from '../client/spec.ts';
import { readJson, sessionPaths, writeJson } from './session.ts';
import type { EnvironmentReport, Footprint, SessionManifest, SupportMatrix } from './types.ts';

export const summarySchemaVersion = 1;

/** Coefficient of variation above which a cell is flagged `noisy`; replace it with the calibrated value. */
export const defaultNoiseThresholdCv = 0.05;

/** The series every other series is compared with. */
export const baselineSeries = 'kafka-core';

/** Series label of the second kafka-core series that the A/A noise test adds. */
export const aaSeries = 'kafka-core#2';

export interface StatSummary {
  samples: number;
  median: number;
  min: number;
  max: number;
  /** Coefficient of variation (sample standard deviation / mean); 0 for a single sample. */
  cv: number;
}

export interface CellMetrics {
  recordsPerSecond: StatSummary | null;
  megabytesPerSecond: StatSummary | null;
  latencyP50Ms: StatSummary | null;
  latencyP99Ms: StatSummary | null;
  cpuSeconds: StatSummary | null;
  recordsPerCpuSecond: StatSummary | null;
  /** cgroup `memory.peak`; null when the container did not expose it. */
  peakMemoryBytes: StatSummary | null;
  rssBytes: StatSummary | null;
  firstAckedProduceMs: StatSummary | null;
  firstConsumedRecordMs: StatSummary | null;
}

export interface CellSummary {
  scenario: ScenarioId;
  scenarioName: string;
  kind: ScenarioKind;
  series: string;
  client: ClientId;
  runtime: RuntimeId;
  profile: Profile;
  clientVersion: string | null;
  /** Records in each run's measured window, as scheduled. */
  messageCount: number;
  runs: number;
  passedRuns: number;
  capabilityGaps: string[];
  primaryMetric: PrimaryMetric;
  metrics: CellMetrics;
  noisy: boolean;
}

export type PrimaryMetric = 'recordsPerSecond' | 'latencyP99Ms' | 'firstAckedProduceMs';

export interface Comparison {
  scenario: ScenarioId;
  runtime: RuntimeId;
  profile: Profile;
  series: string;
  primaryMetric: PrimaryMetric;
  /** Median of `series` divided by the baseline median; above 1 means a larger value. */
  ratio: number;
  /** `better`/`worse` from the series' point of view, accounting for the metric's direction. */
  verdict: 'better' | 'worse' | 'inconclusive';
  reason: string;
}

export interface RunFailure {
  runId: string;
  scenario: ScenarioId;
  series: string;
  runtime: RuntimeId;
  profile: Profile;
  status: string;
  reason: string;
}

export interface SessionSummary {
  schemaVersion: typeof summarySchemaVersion;
  sessionId: string;
  generatedAt: string;
  manifest: SessionManifest;
  environment: EnvironmentReport | null;
  supportMatrix: SupportMatrix | null;
  footprints: Footprint[] | null;
  /** CV threshold used for `noisy`. */
  noiseThresholdCv: number;
  /** Noise floor measured by an earlier A/A session and passed in; null when none was given. */
  noiseFloor: number | null;
  /**
   * Per scenario, the largest relative median difference between `kafka-core` and the A/A series
   * `kafka-core#2` in this session. Empty without `--aa`.
   */
  aaNoiseFloors: Partial<Record<ScenarioId, number>>;
  cells: CellSummary[];
  comparisons: Comparison[];
  failures: RunFailure[];
}

const primaryMetricByKind: Readonly<Record<ScenarioKind, PrimaryMetric>> = {
  produce: 'recordsPerSecond',
  consume: 'recordsPerSecond',
  e2e: 'latencyP99Ms',
  startup: 'firstAckedProduceMs',
};

const lowerIsBetter: ReadonlySet<PrimaryMetric> = new Set(['latencyP99Ms', 'firstAckedProduceMs']);

export function summarize(values: readonly number[]): StatSummary | null {
  if (values.length === 0) {
    return null;
  }
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  const median = sorted.length % 2 === 0 ? (sorted[middle - 1]! + sorted[middle]!) / 2 : sorted[middle]!;
  const mean = sorted.reduce((sum, value) => sum + value, 0) / sorted.length;
  let cv = 0;
  if (sorted.length > 1 && mean !== 0) {
    const variance = sorted.reduce((sum, value) => sum + (value - mean) ** 2, 0) / (sorted.length - 1);
    cv = Math.sqrt(variance) / Math.abs(mean);
  }
  return { samples: sorted.length, median, min: sorted[0]!, max: sorted[sorted.length - 1]!, cv };
}

function collect(records: readonly RunRecord[], pick: (record: RunRecord) => number | null | undefined): number[] {
  const values: number[] = [];
  for (const record of records) {
    const value = pick(record);
    if (typeof value === 'number' && Number.isFinite(value)) {
      values.push(value);
    }
  }
  return values;
}

function summarizeMetrics(passed: readonly RunRecord[]): CellMetrics {
  const metric = (pick: (record: RunRecord) => number | null | undefined) => summarize(collect(passed, pick));
  return {
    recordsPerSecond: metric((record) => record.result.metrics?.recordsPerSecond),
    megabytesPerSecond: metric((record) => record.result.metrics?.megabytesPerSecond),
    latencyP50Ms: metric((record) => record.result.metrics?.latency?.p50Ms),
    latencyP99Ms: metric((record) => record.result.metrics?.latency?.p99Ms),
    cpuSeconds: metric((record) => {
      const cpu = record.result.metrics?.cpu;
      return cpu ? cpu.userSeconds + cpu.systemSeconds : null;
    }),
    recordsPerCpuSecond: metric((record) => record.result.metrics?.recordsPerCpuSecond),
    peakMemoryBytes: metric((record) => record.result.metrics?.peakMemoryBytes),
    rssBytes: metric((record) => record.result.metrics?.rssBytes),
    firstAckedProduceMs: metric((record) => record.result.metrics?.startup?.firstAckedProduceMs),
    firstConsumedRecordMs: metric((record) => record.result.metrics?.startup?.firstConsumedRecordMs),
  };
}

function cellKey(scenario: string, series: string, runtime: string, profile: string): string {
  return `${scenario}|${series}|${runtime}|${profile}`;
}

function summarizeCell(records: readonly RunRecord[], noiseThresholdCv: number): CellSummary {
  const first = records[0]!;
  const { spec } = first;
  const definition = scenarios[spec.scenario];
  const passed = records.filter((record) => record.verification.passed && record.result.status === 'ok');
  const metrics = summarizeMetrics(passed);
  const primaryMetric = primaryMetricByKind[definition.kind];
  const gaps = new Set<string>();
  for (const record of records) {
    for (const gap of record.result.capabilityGaps) {
      gaps.add(gap);
    }
  }
  return {
    scenario: spec.scenario,
    scenarioName: definition.name,
    kind: definition.kind,
    series: spec.series,
    client: spec.client,
    runtime: spec.runtime,
    profile: spec.profile,
    clientVersion: passed[0]?.result.clientVersion ?? first.result.clientVersion,
    messageCount: spec.messageCount,
    runs: records.length,
    passedRuns: passed.length,
    capabilityGaps: [...gaps].sort(),
    primaryMetric,
    metrics,
    noisy: (metrics[primaryMetric]?.cv ?? 0) > noiseThresholdCv,
  };
}

function rangesOverlap(left: StatSummary, right: StatSummary): boolean {
  return left.min <= right.max && right.min <= left.max;
}

function compare(cell: CellSummary, baseline: CellSummary, noiseFloor: number): Comparison | null {
  const metric = cell.primaryMetric;
  const candidate = cell.metrics[metric];
  const reference = baseline.metrics[metric];
  if (!candidate || !reference || reference.median === 0) {
    return null;
  }
  const ratio = candidate.median / reference.median;
  const base = { scenario: cell.scenario, runtime: cell.runtime, profile: cell.profile, series: cell.series };
  const inconclusive = (reason: string): Comparison => ({
    ...base,
    primaryMetric: metric,
    ratio,
    verdict: 'inconclusive',
    reason,
  });
  if (cell.noisy || baseline.noisy) {
    return inconclusive('a cell exceeds the noise threshold');
  }
  if (rangesOverlap(candidate, reference)) {
    return inconclusive('min-max ranges overlap');
  }
  if (Math.abs(ratio - 1) <= noiseFloor) {
    return inconclusive('difference is within the A/A noise floor');
  }
  const isLarger = ratio > 1;
  const isBetter = lowerIsBetter.has(metric) ? !isLarger : isLarger;
  return { ...base, primaryMetric: metric, ratio, verdict: isBetter ? 'better' : 'worse', reason: '' };
}

/**
 * Per scenario, the largest relative median difference between the baseline and the A/A series.
 * Kept per scenario because latency and startup cells are far noisier than throughput cells.
 */
function measureAaNoiseFloors(cellsByKey: ReadonlyMap<string, CellSummary>): Partial<Record<ScenarioId, number>> {
  const floors: Partial<Record<ScenarioId, number>> = {};
  for (const cell of cellsByKey.values()) {
    if (cell.series !== aaSeries) {
      continue;
    }
    const baseline = cellsByKey.get(cellKey(cell.scenario, baselineSeries, cell.runtime, cell.profile));
    const candidate = cell.metrics[cell.primaryMetric];
    const reference = baseline?.metrics[cell.primaryMetric];
    if (!candidate || !reference || reference.median === 0) {
      continue;
    }
    const difference = Math.abs(candidate.median / reference.median - 1);
    floors[cell.scenario] = Math.max(floors[cell.scenario] ?? 0, difference);
  }
  return floors;
}

async function readJsonIfPresent<T>(path: string): Promise<T | null> {
  try {
    return (await readJson(path)) as T;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      return null;
    }
    throw error;
  }
}

export async function readRunRecords(sessionDir: string): Promise<RunRecord[]> {
  const { runsDir } = sessionPaths(sessionDir);
  const names = (await readdir(runsDir)).filter((name) => name.endsWith('.json')).sort();
  const records: RunRecord[] = [];
  for (const name of names) {
    records.push((await readJson(join(runsDir, name))) as RunRecord);
  }
  return records;
}

export interface AggregateOptions {
  /** Defaults to `defaultNoiseThresholdCv`. */
  noiseThresholdCv?: number;
  /** A/A noise floor from an earlier calibration session, as a fraction (0.03 = 3%). */
  noiseFloor?: number;
}

/** Reads a raw session, computes per-cell statistics and comparisons, and writes `summary.json`. */
export async function aggregateSession(sessionDir: string, options: AggregateOptions = {}): Promise<SessionSummary> {
  const noiseThresholdCv = options.noiseThresholdCv ?? defaultNoiseThresholdCv;
  const noiseFloor = options.noiseFloor ?? null;
  const paths = sessionPaths(sessionDir);
  const manifest = await readJsonIfPresent<SessionManifest>(paths.session);
  if (!manifest) {
    throw new Error(`${sessionDir} has no session.json; is it a benchmark session directory?`);
  }
  const records = await readRunRecords(sessionDir);

  const recordsByCell = new Map<string, RunRecord[]>();
  for (const record of records) {
    const { scenario, series, runtime, profile } = record.spec;
    const key = cellKey(scenario, series, runtime, profile);
    const group = recordsByCell.get(key) ?? [];
    group.push(record);
    recordsByCell.set(key, group);
  }

  const cellsByKey = new Map<string, CellSummary>();
  for (const [key, group] of recordsByCell) {
    cellsByKey.set(key, summarizeCell(group, noiseThresholdCv));
  }
  const aaNoiseFloors = measureAaNoiseFloors(cellsByKey);

  const comparisons: Comparison[] = [];
  for (const cell of cellsByKey.values()) {
    // The A/A series defines the noise floor, so comparing it would only restate that floor.
    if (cell.series === baselineSeries || cell.series === aaSeries) {
      continue;
    }
    const baseline = cellsByKey.get(cellKey(cell.scenario, baselineSeries, cell.runtime, cell.profile));
    const floor = Math.max(noiseFloor ?? 0, aaNoiseFloors[cell.scenario] ?? 0);
    const comparison = baseline ? compare(cell, baseline, floor) : null;
    if (comparison) {
      comparisons.push(comparison);
    }
  }

  const failures: RunFailure[] = records
    .filter((record) => !record.verification.passed || record.result.status !== 'ok')
    .map((record) => ({
      runId: record.spec.runId,
      scenario: record.spec.scenario,
      series: record.spec.series,
      runtime: record.spec.runtime,
      profile: record.spec.profile,
      status: record.result.status,
      reason: record.result.error ?? record.verification.details,
    }));

  const cells = [...cellsByKey.values()].sort((left, right) =>
    cellKey(left.scenario, left.runtime, left.profile, left.series).localeCompare(
      cellKey(right.scenario, right.runtime, right.profile, right.series),
    ),
  );

  const summary: SessionSummary = {
    schemaVersion: summarySchemaVersion,
    sessionId: manifest.sessionId,
    generatedAt: new Date().toISOString(),
    manifest,
    environment: await readJsonIfPresent<EnvironmentReport>(paths.environment),
    supportMatrix: await readJsonIfPresent<SupportMatrix>(paths.supportMatrix),
    footprints: await readJsonIfPresent<Footprint[]>(paths.footprint),
    noiseThresholdCv,
    noiseFloor,
    aaNoiseFloors,
    cells,
    comparisons,
    failures,
  };
  await writeJson(paths.summary, summary);
  return summary;
}
