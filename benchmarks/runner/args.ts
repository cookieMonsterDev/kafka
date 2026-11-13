// Command-line flags of runner/cli.ts: the usage text and one parser per command. Parsers return null
// when `--help` was given, and throw `UsageError` for invalid input.

import { randomInt } from 'node:crypto';
import { parseArgs } from 'node:util';
import type { ParseArgsOptionsConfig } from 'node:util';
import { clientIds, payloadSizes, profiles, runtimeIds, scenarioIds, tiers } from '../client/spec.ts';
import type { ClientId, PayloadSize, Profile, RuntimeId, ScenarioId, Tier } from '../client/spec.ts';
import { aaSeries, defaultNoiseThresholdCv } from './aggregate.ts';
import { defaultCoreRef, worktreeRef } from './core-source.ts';
import { defaultProfiles, defaultScenarios } from './schedule.ts';

const maxSeed = 0xffff_ffff;
const maxRepetitions = 1000;
const defaultMinRepetitions = 5;

export const usage = `Usage: node runner/cli.ts <command> [flags]

Commands:
  doctor    Check the host and Docker, print the resource budget.
            --force                 downgrade failed checks to warnings
  probe     Build images, start the broker and probe which client/runtime cells work.
            --core-ref <ref>        kafka-core source: git ref or ${worktreeRef} (default ${defaultCoreRef})
            --clients <list>        comma-separated (default ${clientIds.join(',')})
            --runtimes <list>       comma-separated (default ${runtimeIds.join(',')})
            --force, --keep-broker
  run       Run a benchmark session.
            --tier <tier>           ${tiers.join(' | ')} (default smoke)
            --core-ref <ref>        as for probe
            --clients, --runtimes   as for probe
            --scenarios <list>      default: the scenarios of the tier
            --profiles <list>       ${profiles.join(',')} (default: normalized for smoke, both otherwise)
            --repetitions <n>       measured repetitions per cell (default 5; 1 for smoke)
            --seed <n>              run-order shuffle seed, 0..${maxSeed} (default random, recorded);
                                    record contents always use the fixed payload seed
            --payload <size>        ${payloadSizes.join(' | ')} (default small)
            --count-scale <factor>  multiply message counts and E1 duration (default 1)
            --aa                    add a second kafka-core series (${aaSeries}) for the A/A noise test
            --force                 run despite failed doctor checks (the session cannot be published)
            --keep-broker           leave the broker running afterwards
  report    Aggregate a session and write its Markdown report.
            --session <dir>         default: the latest session
            --noise-threshold <cv>  coefficient of variation above which a cell is flagged noisy (default ${defaultNoiseThresholdCv})
            --noise-floor <frac>    A/A noise floor measured separately, e.g. 0.03 (default: none)
  publish   Publish a session summary to results/published/.
            --session <dir>         default: the latest session
            --min-repetitions <n>   default ${defaultMinRepetitions}
            --noise-threshold, --noise-floor  as for report
  typecheck Type-check client/ and runner/ inside the bench image against the installed clients.
            --core-ref <ref>        as for probe
`;

export class UsageError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'UsageError';
  }
}

function parseList<T extends string>(value: string | undefined, allowed: readonly T[], flag: string): T[] | undefined {
  if (value === undefined) {
    return undefined;
  }
  const items = value
    .split(',')
    .map((item) => item.trim())
    .filter((item) => item !== '');
  const unknown = items.filter((item) => !allowed.includes(item as T));
  if (unknown.length > 0 || items.length === 0) {
    throw new UsageError(`--${flag}: expected a comma-separated subset of ${allowed.join(', ')}, got "${value}"`);
  }
  return [...new Set(items as T[])];
}

function parseChoice<T extends string>(value: string | undefined, allowed: readonly T[], flag: string, fallback: T): T {
  if (value === undefined) {
    return fallback;
  }
  if (!allowed.includes(value as T)) {
    throw new UsageError(`--${flag}: expected one of ${allowed.join(', ')}, got "${value}"`);
  }
  return value as T;
}

function parseInteger(value: string | undefined, flag: string, minimum: number, maximum: number): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  const parsed = Number(value);
  if (!/^\d+$/.test(value) || !Number.isSafeInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new UsageError(`--${flag}: expected an integer from ${minimum} to ${maximum}, got "${value}"`);
  }
  return parsed;
}

function parsePositiveNumber(value: string | undefined, flag: string): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  const parsed = Number(value);
  if (value.trim() === '' || !Number.isFinite(parsed) || parsed <= 0) {
    throw new UsageError(`--${flag}: expected a positive number, got "${value}"`);
  }
  return parsed;
}

function parseFraction(value: string | undefined, flag: string): number | undefined {
  const parsed = parsePositiveNumber(value, flag);
  if (parsed !== undefined && parsed >= 1) {
    throw new UsageError(`--${flag}: expected a fraction between 0 and 1, got "${value}"`);
  }
  return parsed;
}

const helpOption = { help: { type: 'boolean', short: 'h' } } as const satisfies ParseArgsOptionsConfig;

/** Parses strict flags plus `--help`/`-h`; returns null when help was requested. */
function parseFlags<Options extends ParseArgsOptionsConfig>(args: string[], options: Options) {
  const { values } = parseArgs({ args, strict: true, options: { ...options, ...helpOption } });
  // `helpOption` is always merged in; TypeScript cannot see that through the generic `Options`.
  const isHelpRequested = (values as { help?: boolean }).help === true;
  return isHelpRequested ? null : values;
}

const coreSelectionFlags = {
  'core-ref': { type: 'string' },
  clients: { type: 'string' },
  runtimes: { type: 'string' },
} as const satisfies ParseArgsOptionsConfig;

const environmentFlags = {
  ...coreSelectionFlags,
  force: { type: 'boolean' },
  'keep-broker': { type: 'boolean' },
} as const satisfies ParseArgsOptionsConfig;

const reportFlags = {
  session: { type: 'string' },
  'noise-threshold': { type: 'string' },
  'noise-floor': { type: 'string' },
} as const satisfies ParseArgsOptionsConfig;

export interface DoctorArgs {
  force: boolean;
}

/** Flags shared by `probe` and `run`: what to build and probe, and how to treat the environment. */
export interface EnvironmentArgs {
  coreRef: string;
  clients: ClientId[];
  runtimes: RuntimeId[];
  force: boolean;
  keepBroker: boolean;
}

export interface RunArgs extends EnvironmentArgs {
  tier: Tier;
  scenarios: ScenarioId[];
  profiles: Profile[];
  repetitions: number;
  /** Shuffle seed of the schedule. */
  seed: number;
  payload: PayloadSize;
  countScale: number;
  isAaTest: boolean;
}

export interface NoiseOptions {
  noiseThresholdCv: number;
  noiseFloor?: number;
}

export interface ReportArgs {
  /** Session directory; the latest session when undefined. */
  session: string | undefined;
  noise: NoiseOptions;
}

export interface PublishArgs extends ReportArgs {
  minRepetitions: number;
}

export interface TypecheckArgs {
  coreRef: string;
}

function readEnvironmentArgs(values: {
  'core-ref'?: string;
  clients?: string;
  runtimes?: string;
  force?: boolean;
  'keep-broker'?: boolean;
}): EnvironmentArgs {
  return {
    coreRef: values['core-ref'] ?? defaultCoreRef,
    clients: parseList(values.clients, clientIds, 'clients') ?? [...clientIds],
    runtimes: parseList(values.runtimes, runtimeIds, 'runtimes') ?? [...runtimeIds],
    force: values.force === true,
    keepBroker: values['keep-broker'] === true,
  };
}

function readReportArgs(values: { session?: string; 'noise-threshold'?: string; 'noise-floor'?: string }): ReportArgs {
  const noiseFloor = parseFraction(values['noise-floor'], 'noise-floor');
  return {
    session: values.session,
    noise: {
      noiseThresholdCv: parseFraction(values['noise-threshold'], 'noise-threshold') ?? defaultNoiseThresholdCv,
      ...(noiseFloor === undefined ? {} : { noiseFloor }),
    },
  };
}

export function parseDoctorArgs(args: string[]): DoctorArgs | null {
  const values = parseFlags(args, { force: { type: 'boolean' } });
  if (values === null) {
    return null;
  }
  return { force: values.force === true };
}

export function parseProbeArgs(args: string[]): EnvironmentArgs | null {
  const values = parseFlags(args, environmentFlags);
  return values === null ? null : readEnvironmentArgs(values);
}

export function parseRunArgs(args: string[]): RunArgs | null {
  const values = parseFlags(args, {
    ...environmentFlags,
    tier: { type: 'string' },
    scenarios: { type: 'string' },
    profiles: { type: 'string' },
    repetitions: { type: 'string' },
    seed: { type: 'string' },
    payload: { type: 'string' },
    'count-scale': { type: 'string' },
    aa: { type: 'boolean' },
  });
  if (values === null) {
    return null;
  }
  const tier = parseChoice(values.tier, tiers, 'tier', 'smoke');
  return {
    ...readEnvironmentArgs(values),
    tier,
    scenarios: parseList(values.scenarios?.toUpperCase(), scenarioIds, 'scenarios') ?? defaultScenarios(tier),
    profiles: parseList(values.profiles, profiles, 'profiles') ?? defaultProfiles(tier),
    repetitions: parseInteger(values.repetitions, 'repetitions', 1, maxRepetitions) ?? (tier === 'smoke' ? 1 : 5),
    seed: parseInteger(values.seed, 'seed', 0, maxSeed) ?? randomInt(0, maxSeed + 1),
    payload: parseChoice(values.payload, payloadSizes, 'payload', 'small'),
    countScale: parsePositiveNumber(values['count-scale'], 'count-scale') ?? 1,
    isAaTest: values.aa === true,
  };
}

export function parseReportArgs(args: string[]): ReportArgs | null {
  const values = parseFlags(args, reportFlags);
  return values === null ? null : readReportArgs(values);
}

export function parsePublishArgs(args: string[]): PublishArgs | null {
  const values = parseFlags(args, { ...reportFlags, 'min-repetitions': { type: 'string' } });
  if (values === null) {
    return null;
  }
  return {
    ...readReportArgs(values),
    minRepetitions:
      parseInteger(values['min-repetitions'], 'min-repetitions', 1, maxRepetitions) ?? defaultMinRepetitions,
  };
}

export function parseTypecheckArgs(args: string[]): TypecheckArgs | null {
  const values = parseFlags(args, { 'core-ref': { type: 'string' } });
  return values === null ? null : { coreRef: values['core-ref'] ?? defaultCoreRef };
}
