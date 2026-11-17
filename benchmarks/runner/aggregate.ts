import { readdir } from 'node:fs/promises';
import { join } from 'node:path';

import { scenarios } from '../client/spec.ts';
import type { ClientId, Profile, RunRecord, RuntimeId, ScenarioId, ScenarioKind } from '../client/spec.ts';
import { readJson, sessionPaths, writeJson } from './session.ts';
import type { EnvironmentReport, Footprint, SessionManifest, SupportMatrix } from './types.ts';

export const summarySchemaVersion = 3;

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

/** A metric the comparison rule runs on: each scenario's primary metric, plus the E1 median. */
export type ComparedMetric = PrimaryMetric | 'latencyP50Ms';

export type ComparisonVerdict = 'better' | 'worse' | 'inconclusive';

export interface StatComparison {
  /** Candidate median divided by the reference median; above 1 means a larger value. */
  ratio: number;
  /** `better`/`worse` from the candidate's point of view, accounting for the metric's direction. */
  verdict: ComparisonVerdict;
  /** Why the verdict is `inconclusive`; empty otherwise. */
  reason: string;
}

export interface Comparison extends StatComparison {
  scenario: ScenarioId;
  runtime: RuntimeId;
  profile: Profile;
  series: string;
  metric: ComparedMetric;
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
   * Per scenario and compared metric, the largest relative median difference between `kafka-core` and the
   * A/A series `kafka-core#2` in this session. Empty without `--aa`.
   */
  aaNoiseFloors: AaNoiseFloors;
  cells: CellSummary[];
  comparisons: Comparison[];
  failures: RunFailure[];
}

export type AaNoiseFloors = Partial<Record<ScenarioId, Partial<Record<ComparedMetric, number>>>>;

const primaryMetricByKind: Readonly<Record<ScenarioKind, PrimaryMetric>> = {
  produce: 'recordsPerSecond',
  consume: 'recordsPerSecond',
  e2e: 'latencyP99Ms',
  startup: 'firstAckedProduceMs',
};

/**
 * Metrics compared besides the primary one. E1 medians of different clients sit tens of microseconds apart,
 * well inside their run-to-run spread, so a lowest median is named only when the same rule separates it.
 */
const secondaryMetricsByKind: Readonly<Partial<Record<ScenarioKind, readonly ComparedMetric[]>>> = {
  e2e: ['latencyP50Ms'],
};

const lowerIsBetter: ReadonlySet<ComparedMetric> = new Set(['latencyP50Ms', 'latencyP99Ms', 'firstAckedProduceMs']);

/**
 * The noise floor a comparison of `metric` in `scenario` uses: the calibrated floor passed in or the A/A
 * floor measured in the session, whichever is larger.
 */
export function effectiveNoiseFloor(
  summary: Pick<SessionSummary, 'noiseFloor' | 'aaNoiseFloors'>,
  scenario: ScenarioId,
  metric: ComparedMetric,
): number {
  return Math.max(summary.noiseFloor ?? 0, summary.aaNoiseFloors[scenario]?.[metric] ?? 0);
}

function comparedMetrics(cell: CellSummary): ComparedMetric[] {
  return [cell.primaryMetric, ...(secondaryMetricsByKind[cell.kind] ?? [])];
}

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

/** The statistics the comparison rule reads from one cell's metric. */
export type ComparedStat = Pick<StatSummary, 'median' | 'min' | 'max' | 'cv'>;

function rangesOverlap(left: ComparedStat, right: ComparedStat): boolean {
  return left.min <= right.max && right.min <= left.max;
}

export interface ComparisonRule {
  /** Coefficient of variation above which either side makes the comparison inconclusive. */
  noiseThresholdCv: number;
  /** Relative median difference (0.03 = 3%) at or below which the medians count as equal. */
  noiseFloor: number;
  isLowerBetter: boolean;
}

/**
 * Compares a candidate with a reference: inconclusive when either side is noisy, their min–max ranges
 * overlap, or their medians differ by no more than the noise floor. Null when the reference median is 0.
 */
export function compareStats(
  candidate: ComparedStat,
  reference: ComparedStat,
  rule: ComparisonRule,
): StatComparison | null {
  if (reference.median === 0) {
    return null;
  }
  const ratio = candidate.median / reference.median;
  const inconclusive = (reason: string): StatComparison => ({ ratio, verdict: 'inconclusive', reason });
  if (candidate.cv > rule.noiseThresholdCv || reference.cv > rule.noiseThresholdCv) {
    return inconclusive('a cell exceeds the noise threshold');
  }
  if (rangesOverlap(candidate, reference)) {
    return inconclusive('min-max ranges overlap');
  }
  if (Math.abs(ratio - 1) <= rule.noiseFloor) {
    return inconclusive('difference is within the A/A noise floor');
  }
  const isLarger = ratio > 1;
  const isBetter = rule.isLowerBetter ? !isLarger : isLarger;
  return { ratio, verdict: isBetter ? 'better' : 'worse', reason: '' };
}

/**
 * The candidate that compares `better` than every other candidate, or null when no single one does: fewer
 * than two candidates, or any comparison with the best median is inconclusive.
 */
export function findClearBest<T extends ComparedStat>(candidates: readonly T[], rule: ComparisonRule): T | null {
  const [first, ...rest] = candidates;
  if (first === undefined || rest.length === 0) {
    return null;
  }
  const isBetterMedian = (left: T, right: T) =>
    rule.isLowerBetter ? left.median < right.median : left.median > right.median;
  const best = rest.reduce((leader, candidate) => (isBetterMedian(candidate, leader) ? candidate : leader), first);
  const isClear = candidates.every((other) => other === best || compareStats(best, other, rule)?.verdict === 'better');
  return isClear ? best : null;
}

function compare(
  cell: CellSummary,
  baseline: CellSummary,
  metric: ComparedMetric,
  rule: ComparisonRule,
): Comparison | null {
  const candidate = cell.metrics[metric];
  const reference = baseline.metrics[metric];
  if (!candidate || !reference) {
    return null;
  }
  const comparison = compareStats(candidate, reference, rule);
  if (!comparison) {
    return null;
  }
  return {
    scenario: cell.scenario,
    runtime: cell.runtime,
    profile: cell.profile,
    series: cell.series,
    metric,
    ...comparison,
  };
}

/**
 * Per scenario and compared metric, the largest relative median difference between the baseline and the
 * A/A series. Kept apart because latency and startup cells are far noisier than throughput cells, and an
 * E1 p99 is far noisier than its median.
 */
function measureAaNoiseFloors(cellsByKey: ReadonlyMap<string, CellSummary>): AaNoiseFloors {
  const floors: AaNoiseFloors = {};
  for (const cell of cellsByKey.values()) {
    if (cell.series !== aaSeries) {
      continue;
    }
    const baseline = cellsByKey.get(cellKey(cell.scenario, baselineSeries, cell.runtime, cell.profile));
    for (const metric of comparedMetrics(cell)) {
      const candidate = cell.metrics[metric];
      const reference = baseline?.metrics[metric];
      if (!candidate || !reference || reference.median === 0) {
        continue;
      }
      const difference = Math.abs(candidate.median / reference.median - 1);
      const scenarioFloors = (floors[cell.scenario] ??= {});
      scenarioFloors[metric] = Math.max(scenarioFloors[metric] ?? 0, difference);
    }
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

/** A `footprint.json` entry written before F1 split required and optional dependencies. */
interface UnsplitFootprintEntry {
  client: Footprint['client'];
  packageName: string;
  version: string | null;
  /** Null, like the two fields below, when the package is not installed. */
  installBytes: number | null;
  dependencyCount: number | null;
  hasNativeAddon: boolean | null;
}

/** Keeps the total of an unsplit entry as it was measured; it cannot be divided into the two parts. */
function fromUnsplitEntry(entry: UnsplitFootprintEntry): Footprint {
  const { client, packageName, version, installBytes, dependencyCount, hasNativeAddon } = entry;
  const footprint: Footprint = { client, packageName, version, required: null, optional: null };
  if (installBytes !== null && dependencyCount !== null && hasNativeAddon !== null) {
    footprint.unsplitTotal = { bytes: installBytes, dependencyCount, hasNativeAddon };
  }
  return footprint;
}

/** The session's F1 footprints; entries from a session recorded before the split keep only their total. */
async function readFootprints(path: string): Promise<Footprint[] | null> {
  const entries = await readJsonIfPresent<(Footprint | UnsplitFootprintEntry)[]>(path);
  return entries?.map((entry) => ('required' in entry ? entry : fromUnsplitEntry(entry))) ?? null;
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
    if (!baseline) {
      continue;
    }
    for (const metric of comparedMetrics(cell)) {
      const rule: ComparisonRule = {
        noiseThresholdCv,
        noiseFloor: effectiveNoiseFloor({ noiseFloor, aaNoiseFloors }, cell.scenario, metric),
        isLowerBetter: lowerIsBetter.has(metric),
      };
      const comparison = compare(cell, baseline, metric, rule);
      if (comparison) {
        comparisons.push(comparison);
      }
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
    footprints: await readFootprints(paths.footprint),
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
