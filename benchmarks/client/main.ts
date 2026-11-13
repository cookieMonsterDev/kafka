// Runs one benchmark (or the support probe) inside the client container:
//   node client/main.ts [probe]   |   bun client/main.ts [probe]
// Reads the RunSpec from BENCH_SPEC and prints exactly one result line to stdout; everything else
// goes to stderr.
import { AdapterLoadError, isUnsupportedError, loadAdapter } from './adapters/index.ts';
import type { AdapterOptions } from './adapters/types.ts';
import { createRecordPool } from './payload.ts';
import { runProbe } from './scenarios/probe.ts';
import { scenarioRunners } from './scenarios/index.ts';
import { ClientTracker, describeError, type ObservedCounts } from './scenarios/harness.ts';
import {
  acksLevels,
  type ClientResult,
  clientIds,
  compressions,
  payloadSizes,
  profiles,
  type RunMetrics,
  type RunSpec,
  type RunStatus,
  type RuntimeId,
  resultLineMarker,
  resultSchemaVersion,
  runtimeIds,
  scenarioIds,
  scenarios,
  specEnvVar,
  type WireSettings,
} from './spec.ts';

const disconnectGraceMs = 5_000;
const exitFallbackMs = 2_000;
const benchClientId = 'kafka-bench';

type Mode = 'run' | 'probe';

// Some libraries log to stdout; only the result line may go there.
function routeConsoleToStderr(): void {
  const toStderr = (...values: unknown[]): void => console.error(...values);
  console.log = toStderr;
  console.info = toStderr;
  console.debug = toStderr;
  console.warn = toStderr;
  console.trace = toStderr;
}

function parseMode(args: readonly string[]): Mode {
  if (args.length === 0) {
    return 'run';
  }
  if (args.length === 1 && args[0] === 'probe') {
    return 'probe';
  }
  throw new Error(`unexpected arguments: ${args.join(' ')} (usage: main.ts [probe])`);
}

interface RuntimeInfo {
  name: 'node' | 'bun';
  version: string;
  id: RuntimeId | null;
}

function detectRuntime(): RuntimeInfo {
  const bunVersion = process.versions.bun;
  if (bunVersion !== undefined) {
    return { name: 'bun', version: bunVersion, id: 'bun' };
  }
  const version = process.versions.node;
  const major = version.split('.')[0];
  const id = runtimeIds.find((candidate) => candidate === `node${major}`) ?? null;
  return { name: 'node', version, id };
}

type JsonObject = Record<string, unknown>;

function isObject(value: unknown): value is JsonObject {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function requireString(object: JsonObject, key: string): string {
  const value = object[key];
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`${key} must be a non-empty string`);
  }
  return value;
}

function requireNumber(
  object: JsonObject,
  key: string,
  minimum: number,
  { isInteger = true }: { isInteger?: boolean } = {},
): number {
  const value = object[key];
  if (typeof value !== 'number' || !Number.isFinite(value) || value < minimum) {
    throw new Error(`${key} must be a number >= ${minimum}`);
  }
  if (isInteger && !Number.isInteger(value)) {
    throw new Error(`${key} must be an integer`);
  }
  return value;
}

function requireOneOf<T extends string | number | boolean>(object: JsonObject, key: string, values: readonly T[]): T {
  const value = object[key];
  const match = values.find((candidate) => candidate === value);
  if (match === undefined) {
    throw new Error(`${key} must be one of ${values.join(', ')}`);
  }
  return match;
}

function parseWire(value: unknown): WireSettings {
  if (!isObject(value)) {
    throw new Error('wire must be an object');
  }
  return {
    acks: requireOneOf(value, 'acks', acksLevels),
    lingerMs: requireNumber(value, 'lingerMs', 0),
    batchBytes: requireNumber(value, 'batchBytes', 0),
    compression: requireOneOf(value, 'compression', compressions),
    maxInFlight: requireNumber(value, 'maxInFlight', 1),
    idempotent: requireOneOf(value, 'idempotent', [true, false] as const),
    fetchMinBytes: requireNumber(value, 'fetchMinBytes', 0),
    fetchMaxBytes: requireNumber(value, 'fetchMaxBytes', 1),
    fetchMaxWaitMs: requireNumber(value, 'fetchMaxWaitMs', 0),
    partitionFetchMaxBytes: requireNumber(value, 'partitionFetchMaxBytes', 1),
  };
}

function wireDifferences(actual: WireSettings, expected: WireSettings): string[] {
  const keys = Object.keys(expected) as (keyof WireSettings)[];
  return keys.filter((key) => actual[key] !== expected[key]).map((key) => `${key}=${String(actual[key])}`);
}

function parseRunSpec(raw: string | undefined): RunSpec {
  if (raw === undefined || raw.trim() === '') {
    throw new Error(`${specEnvVar} is not set`);
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (error) {
    throw new Error(`${specEnvVar} is not valid JSON: ${describeError(error)}`, { cause: error });
  }
  if (!isObject(parsed)) {
    throw new Error(`${specEnvVar} must be a JSON object`);
  }
  const brokers = parsed.brokers;
  if (!Array.isArray(brokers) || brokers.length === 0 || !brokers.every((broker) => typeof broker === 'string')) {
    throw new Error('brokers must be a non-empty array of host:port strings');
  }

  const spec: RunSpec = {
    runId: requireString(parsed, 'runId'),
    scenario: requireOneOf(parsed, 'scenario', scenarioIds),
    client: requireOneOf(parsed, 'client', clientIds),
    runtime: requireOneOf(parsed, 'runtime', runtimeIds),
    profile: requireOneOf(parsed, 'profile', profiles),
    series: requireString(parsed, 'series'),
    repetition: requireNumber(parsed, 'repetition', 0),
    brokers: brokers as string[],
    topic: requireString(parsed, 'topic'),
    partitions: requireNumber(parsed, 'partitions', 1),
    groupId: requireString(parsed, 'groupId'),
    payload: requireOneOf(parsed, 'payload', payloadSizes),
    seed: requireNumber(parsed, 'seed', 0),
    messageCount: requireNumber(parsed, 'messageCount', 0),
    warmupCount: requireNumber(parsed, 'warmupCount', 0),
    rateMessagesPerSecond: requireNumber(parsed, 'rateMessagesPerSecond', 0, { isInteger: false }),
    durationMs: requireNumber(parsed, 'durationMs', 0),
    wire: parseWire(parsed.wire),
    timeoutMs: requireNumber(parsed, 'timeoutMs', 1),
  };

  const definition = scenarios[spec.scenario];
  const differences = wireDifferences(spec.wire, definition.wire);
  if (differences.length > 0) {
    throw new Error(`wire differs from the ${spec.scenario} catalog entry: ${differences.join(', ')}`);
  }
  if (!definition.profiles.includes(spec.profile)) {
    throw new Error(`${spec.scenario} does not run under the ${spec.profile} profile`);
  }
  if (definition.kind === 'e2e' && (spec.rateMessagesPerSecond <= 0 || spec.durationMs <= 0)) {
    throw new Error('E1 needs rateMessagesPerSecond > 0 and durationMs > 0');
  }
  return spec;
}

interface RunState {
  runtime: RuntimeInfo;
  spec: RunSpec | null;
  clientVersion: string | null;
  capabilityGaps: string[];
  observed: ObservedCounts;
  clients: ClientTracker;
  finished: boolean;
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number, fallback: T): Promise<T> {
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve(fallback), timeoutMs);
    void promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      () => {
        clearTimeout(timer);
        resolve(fallback);
      },
    );
  });
}

/** Disconnects clients, prints the single result line and exits; later calls are ignored. */
async function finish(
  state: RunState,
  status: RunStatus,
  error: string | null,
  metrics: RunMetrics | null,
): Promise<void> {
  if (state.finished) {
    return;
  }
  state.finished = true;

  const disconnectErrors = await withTimeout(state.clients.disconnectAll(), disconnectGraceMs, [
    `disconnect did not finish within ${disconnectGraceMs} ms`,
  ]);
  for (const message of disconnectErrors) {
    console.error(message);
  }

  const result: ClientResult = {
    schemaVersion: resultSchemaVersion,
    runId: state.spec?.runId ?? 'unknown',
    status,
    error,
    runtime: {
      id: state.spec?.runtime ?? state.runtime.id ?? (state.runtime.name === 'bun' ? 'bun' : 'node26'),
      name: state.runtime.name,
      version: state.runtime.version,
    },
    clientVersion: state.clientVersion,
    capabilityGaps: state.capabilityGaps,
    observed: { ...state.observed },
    metrics: status === 'ok' ? metrics : null,
  };
  const exitCode = status === 'ok' ? 0 : 1;
  setTimeout(() => process.exit(exitCode), exitFallbackMs);
  process.stdout.write(`${resultLineMarker}${JSON.stringify(result)}\n`, () => process.exit(exitCode));
}

function failureStatus(error: unknown): RunStatus {
  if (error instanceof AdapterLoadError) {
    return error.unsupported ? 'unsupported' : 'failed';
  }
  return isUnsupportedError(error) ? 'unsupported' : 'failed';
}

function describeProgress(observed: ObservedCounts): string {
  return `produced ${observed.producedRecords}, consumed ${observed.consumedRecords} records`;
}

async function execute(state: RunState, mode: Mode, spec: RunSpec): Promise<RunMetrics | null> {
  if (state.runtime.id !== spec.runtime) {
    throw new Error(`spec.runtime is ${spec.runtime} but this is ${state.runtime.name} ${state.runtime.version}`);
  }
  const definition = scenarios[spec.scenario];
  const records = createRecordPool(spec.payload, spec.seed);
  const adapter = await loadAdapter(spec.client);
  try {
    state.clientVersion = adapter.version();
  } catch (error) {
    console.error(`cannot read the ${adapter.packageName} version: ${describeError(error)}`);
  }

  const adapterOptions: AdapterOptions = {
    brokers: spec.brokers,
    clientId: benchClientId,
    profile: spec.profile,
    wire: definition.wire,
  };
  state.capabilityGaps = adapter.capabilityGaps(adapterOptions, definition);

  const context = {
    spec,
    definition,
    adapter,
    adapterOptions,
    records,
    clients: state.clients,
    observed: state.observed,
  };
  if (mode === 'probe') {
    return runProbe(context);
  }
  return scenarioRunners[spec.scenario](context);
}

async function main(): Promise<void> {
  routeConsoleToStderr();
  const state: RunState = {
    runtime: detectRuntime(),
    spec: null,
    clientVersion: null,
    capabilityGaps: [],
    observed: { producedRecords: 0, consumedRecords: 0, consumedBytes: 0 },
    clients: new ClientTracker(),
    finished: false,
  };
  const fail = (error: unknown): void => {
    console.error(error);
    void finish(state, failureStatus(error), describeError(error), null);
  };
  process.on('uncaughtException', fail);
  process.on('unhandledRejection', fail);

  try {
    const mode = parseMode(process.argv.slice(2));
    const spec = parseRunSpec(process.env[specEnvVar]);
    state.spec = spec;
    const deadline = setTimeout(() => {
      const message = `timed out after ${spec.timeoutMs} ms (${describeProgress(state.observed)})`;
      void finish(state, 'failed', message, null);
    }, spec.timeoutMs);
    const metrics = await execute(state, mode, spec);
    clearTimeout(deadline);
    await finish(state, 'ok', null, metrics);
  } catch (error) {
    fail(error);
  }
}

await main();
