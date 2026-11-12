// Matrix expansion and seeded interleaving: every round runs each cell once, in an order shuffled
// from the session seed, so thermal and background drift spreads across clients instead of biasing one.

import { createRandom } from '../client/payload.ts';
import { payloadSeed, scenarioIds, scenarios } from '../client/spec.ts';
import type { ClientId, PayloadSize, Profile, RunSpec, RuntimeId, ScenarioId, Tier } from '../client/spec.ts';
import { aaSeries } from './aggregate.ts';
import type { SupportMatrix } from './types.ts';

export const topicPartitions = 3;

/**
 * Per-run sizes. A produce run writes `warmupCount + messageCount` records; a consume run's topic is
 * seeded with `warmupCount + messageCount` records. For E1, `messageCount` is `rate × duration`.
 */
export interface RunCounts {
  messageCount: number;
  warmupCount: number;
  rateMessagesPerSecond: number;
  durationMs: number;
  timeoutMs: number;
}

function sized(messageCount: number, warmupCount: number, timeoutMs: number): RunCounts {
  return { messageCount, warmupCount, rateMessagesPerSecond: 0, durationMs: 0, timeoutMs };
}

function rated(rateMessagesPerSecond: number, durationMs: number, warmupCount: number, timeoutMs: number): RunCounts {
  const messageCount = Math.round((rateMessagesPerSecond * durationMs) / 1000);
  return { messageCount, warmupCount, rateMessagesPerSecond, durationMs, timeoutMs };
}

// Smoke checks the harness in minutes. Standard counts are first estimates aiming at a 10 to 20 s
// measured window for the slowest client; calibration tunes them (or use --count-scale).
const smokeCounts: Readonly<Record<ScenarioId, RunCounts>> = {
  P1: sized(2_000, 200, 120_000),
  P2: sized(20_000, 2_000, 120_000),
  P3: sized(50_000, 5_000, 120_000),
  P4: sized(50_000, 5_000, 120_000),
  P5: sized(50_000, 5_000, 120_000),
  C1: sized(50_000, 5_000, 120_000),
  C2: sized(50_000, 5_000, 120_000),
  C3: sized(50_000, 5_000, 120_000),
  C4: sized(50_000, 5_000, 120_000),
  E1: rated(1_000, 5_000, 500, 120_000),
  L1: sized(1, 0, 60_000),
};

const standardCounts: Readonly<Record<ScenarioId, RunCounts>> = {
  P1: sized(30_000, 2_000, 300_000),
  P2: sized(500_000, 20_000, 300_000),
  P3: sized(1_000_000, 50_000, 300_000),
  P4: sized(500_000, 25_000, 300_000),
  P5: sized(1_000_000, 50_000, 300_000),
  C1: sized(2_000_000, 50_000, 300_000),
  C2: sized(2_000_000, 50_000, 300_000),
  C3: sized(1_000_000, 50_000, 300_000),
  C4: sized(1_000_000, 50_000, 300_000),
  E1: rated(5_000, 15_000, 5_000, 120_000),
  L1: sized(1, 0, 60_000),
};

export const tierCounts: Readonly<Record<Tier, Readonly<Record<ScenarioId, RunCounts>>>> = {
  smoke: smokeCounts,
  standard: standardCounts,
  full: standardCounts,
};

// Medium records are about 10 times larger; fewer of them keep the broker's 2 GiB tmpfs safe.
const payloadCountFactor: Readonly<Record<PayloadSize, number>> = { small: 1, medium: 0.25 };

/** Counts for one scenario after `countScale` and the payload factor; L1 is never scaled. */
export function runCounts(tier: Tier, scenario: ScenarioId, countScale: number, payload: PayloadSize): RunCounts {
  const base = tierCounts[tier][scenario];
  const kind = scenarios[scenario].kind;
  if (kind === 'startup') {
    return base;
  }
  const timeoutMs = Math.round(base.timeoutMs * Math.max(1, countScale));
  if (kind === 'e2e') {
    const durationMs = Math.max(1000, Math.round(base.durationMs * countScale));
    return rated(base.rateMessagesPerSecond, durationMs, base.warmupCount, timeoutMs);
  }
  const factor = countScale * payloadCountFactor[payload];
  return sized(Math.max(1, Math.round(base.messageCount * factor)), Math.round(base.warmupCount * factor), timeoutMs);
}

export function defaultScenarios(tier: Tier): ScenarioId[] {
  return scenarioIds.filter((id) => scenarios[id].tiers.includes(tier));
}

/** Smoke checks the harness with the headline profile only; the larger tiers add `defaults`. */
export function defaultProfiles(tier: Tier): Profile[] {
  return tier === 'smoke' ? ['normalized'] : ['normalized', 'defaults'];
}

export interface Series {
  client: ClientId;
  label: string;
}

export function seriesFor(clients: readonly ClientId[], isAaTest: boolean): Series[] {
  const series: Series[] = clients.map((client) => ({ client, label: client }));
  if (isAaTest && clients.includes('kafka-core')) {
    series.push({ client: 'kafka-core', label: aaSeries });
  }
  return series;
}

export interface ScheduleCell {
  scenario: ScenarioId;
  client: ClientId;
  series: string;
  runtime: RuntimeId;
  profile: Profile;
}

export interface ScheduleOptions {
  tier: Tier;
  /** Defaults to the scenarios whose `tiers` include `tier`. */
  scenarios?: readonly ScenarioId[];
  clients: readonly ClientId[];
  runtimes: readonly RuntimeId[];
  /** Defaults to `defaultProfiles(tier)`; each scenario still runs only under its own `profiles`. */
  profiles?: readonly Profile[];
  isAaTest: boolean;
  repetitions: number;
  /** Seed of the per-round shuffle only; record contents always come from `payloadSeed`. */
  seed: number;
  /** Cells marked unsupported here are dropped; null keeps every cell. */
  support: SupportMatrix | null;
  payload: PayloadSize;
  countScale: number;
  brokers: readonly string[];
  /** Short filesystem-safe tag that makes run ids (and so topics) unique across sessions. */
  runTag: string;
}

function isSupported(support: SupportMatrix | null, client: ClientId, runtime: RuntimeId): boolean {
  if (support === null) {
    return true;
  }
  const cell = support.cells.find((candidate) => candidate.client === client && candidate.runtime === runtime);
  return cell?.supported !== false;
}

export function expandCells(options: ScheduleOptions): ScheduleCell[] {
  const scenarioList = options.scenarios ?? defaultScenarios(options.tier);
  const profileList = options.profiles ?? defaultProfiles(options.tier);
  const cells: ScheduleCell[] = [];
  for (const scenario of scenarioList) {
    const profiles = profileList.filter((profile) => scenarios[scenario].profiles.includes(profile));
    for (const series of seriesFor(options.clients, options.isAaTest)) {
      for (const runtime of options.runtimes) {
        if (!isSupported(options.support, series.client, runtime)) {
          continue;
        }
        for (const profile of profiles) {
          cells.push({ scenario, client: series.client, series: series.label, runtime, profile });
        }
      }
    }
  }
  return cells;
}

function toIdPart(text: string): string {
  return text.toLowerCase().replace(/[^a-z0-9]+/g, '-');
}

export interface RunSpecContext {
  tier: Tier;
  payload: PayloadSize;
  countScale: number;
  brokers: readonly string[];
}

export interface RunSpecOptions {
  cell: ScheduleCell;
  /** 1-based round number; 0 for runs outside the measured schedule, such as the broker warm-up. */
  repetition: number;
  /** Unique and filesystem-safe (lowercase, digits, dashes); also names the topic and group. */
  runId: string;
  context: RunSpecContext;
}

export function createRunSpec(options: RunSpecOptions): RunSpec {
  const { cell, repetition, runId, context } = options;
  const counts = runCounts(context.tier, cell.scenario, context.countScale, context.payload);
  return {
    runId,
    scenario: cell.scenario,
    client: cell.client,
    runtime: cell.runtime,
    profile: cell.profile,
    series: cell.series,
    repetition,
    brokers: [...context.brokers],
    topic: `bench-${runId}`,
    partitions: topicPartitions,
    groupId: `bench-${runId}`,
    payload: context.payload,
    seed: payloadSeed,
    messageCount: counts.messageCount,
    warmupCount: counts.warmupCount,
    rateMessagesPerSecond: counts.rateMessagesPerSecond,
    durationMs: counts.durationMs,
    wire: scenarios[cell.scenario].wire,
    timeoutMs: counts.timeoutMs,
  };
}

function shuffleInPlace<T>(items: T[], random: () => number): void {
  for (let index = items.length - 1; index > 0; index--) {
    const swapIndex = Math.floor(random() * (index + 1));
    const item = items[index] as T;
    items[index] = items[swapIndex] as T;
    items[swapIndex] = item;
  }
}

/** All runs of a session in execution order: `repetitions` rounds, each a seeded shuffle of every cell. */
export function buildSchedule(options: ScheduleOptions): RunSpec[] {
  const cells = expandCells(options);
  const random = createRandom(options.seed);
  const specs: RunSpec[] = [];
  for (let repetition = 1; repetition <= options.repetitions; repetition++) {
    const round = [...cells];
    shuffleInPlace(round, random);
    for (const cell of round) {
      const sequence = String(specs.length + 1).padStart(4, '0');
      const runId = [options.runTag, sequence, cell.scenario, cell.series, cell.runtime, cell.profile, `r${repetition}`]
        .map(toIdPart)
        .join('-');
      specs.push(createRunSpec({ cell, repetition, runId, context: options }));
    }
  }
  return specs;
}
