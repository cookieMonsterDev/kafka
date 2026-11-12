// Executes scheduled runs: topic, seeding, client container, verification, topic deletion and cool-down.

import { payloadShapes } from '../client/payload.ts';
import { resultSchemaVersion, scenarios } from '../client/spec.ts';
import type {
  ClientResult,
  PayloadSize,
  RunRecord,
  RunSpec,
  RuntimeId,
  ScenarioId,
  Verification,
} from '../client/spec.ts';
import { readEndOffsetTotal, seedTopic, waitForBrokerIdle } from './broker.ts';
import type { BenchmarkAttempts } from './attempt.ts';
import { clientContainerOptions } from './container.ts';
import { errorMessage, tailLines } from './process.ts';
import { createRunSpec } from './schedule.ts';
import { runRecordPath, writeJson } from './session.ts';
import type {
  BrokerHandle,
  ClientContainerOptions,
  ClientContainerOutcome,
  ResourceBudget,
  RuntimeImage,
  SupportMatrix,
} from './types.ts';

const containerTimeoutGraceMs = 30_000;
const coolDownMaxWaitMs = 60_000;
const coolDownCpuThresholdPercent = 10;
const maxConsecutiveInfrastructureFailures = 3;
/** The startup scenario (L1) produces exactly one record. */
const startupRecordCount = 1;
const warmupScenarios: readonly ScenarioId[] = ['P3', 'C1'];

export interface RunContext {
  broker: BrokerHandle;
  budget: ResourceBudget;
  imageByRuntime: ReadonlyMap<RuntimeId, RuntimeImage>;
  log: (line: string) => void;
  attempts: BenchmarkAttempts;
  /** Checked before each scheduled run; true stops the schedule early. */
  isStopping: () => boolean;
}

interface RunAttempt {
  record: RunRecord;
  /** Set when the harness (broker, topic, seeding) failed rather than the client. */
  infrastructureError: string | null;
}

function failedResult(spec: RunSpec, image: RuntimeImage | undefined, error: string): ClientResult {
  return {
    schemaVersion: resultSchemaVersion,
    runId: spec.runId,
    status: 'failed',
    error,
    runtime: { id: spec.runtime, name: spec.runtime === 'bun' ? 'bun' : 'node', version: image?.runtimeVersion ?? '' },
    clientVersion: image?.clientVersions[spec.client] ?? null,
    capabilityGaps: [],
    observed: { producedRecords: 0, consumedRecords: 0, consumedBytes: 0 },
    metrics: null,
  };
}

function describeMissingResult(outcome: ClientContainerOutcome, timeoutMs: number): string {
  if (outcome.timedOut) {
    return `container killed after ${timeoutMs} ms timeout`;
  }
  const lastErrorLines = tailLines(outcome.stderrTail, 3).split('\n').join(' | ');
  return `no valid result line (exit code ${outcome.exitCode ?? 'none'}): ${lastErrorLines || 'no stderr output'}`;
}

function check(problems: string[], isValid: boolean, problem: string): void {
  if (!isValid) {
    problems.push(problem);
  }
}

async function verifyRun(spec: RunSpec, result: ClientResult, broker: BrokerHandle): Promise<Verification> {
  if (result.runId !== spec.runId) {
    return { passed: false, details: `result is for run ${result.runId}, expected ${spec.runId}` };
  }
  if (result.status !== 'ok') {
    return { passed: false, details: `client status ${result.status}: ${result.error ?? 'no error message'}` };
  }
  const expectedRecords = spec.warmupCount + spec.messageCount;
  const { producedRecords, consumedRecords, consumedBytes } = result.observed;
  const problems: string[] = [];
  let summary: string;
  switch (scenarios[spec.scenario].kind) {
    case 'produce': {
      const endOffsets = await readEndOffsetTotal(broker, spec.topic);
      check(problems, endOffsets === expectedRecords, `end offsets ${endOffsets} != expected ${expectedRecords}`);
      check(problems, producedRecords === endOffsets, `client produced ${producedRecords}, end offsets ${endOffsets}`);
      summary = `end offsets ${endOffsets} = expected`;
      break;
    }
    case 'e2e': {
      const endOffsets = await readEndOffsetTotal(broker, spec.topic);
      check(problems, producedRecords > 0, 'client produced nothing');
      check(problems, endOffsets === producedRecords, `end offsets ${endOffsets} != produced ${producedRecords}`);
      check(
        problems,
        consumedRecords === producedRecords,
        `consumed ${consumedRecords} != produced ${producedRecords}`,
      );
      summary = `end offsets ${endOffsets} = produced = consumed`;
      break;
    }
    case 'consume': {
      const expectedBytes = expectedRecords * payloadShapes[spec.payload].valueBytes;
      check(problems, consumedRecords === expectedRecords, `consumed ${consumedRecords} != seeded ${expectedRecords}`);
      check(problems, consumedBytes === expectedBytes, `consumed ${consumedBytes} bytes != seeded ${expectedBytes}`);
      summary = `consumed ${consumedRecords} records, ${consumedBytes} bytes = seeded`;
      break;
    }
    case 'startup': {
      const endOffsets = await readEndOffsetTotal(broker, spec.topic);
      check(
        problems,
        producedRecords === startupRecordCount,
        `client produced ${producedRecords}, expected ${startupRecordCount}`,
      );
      check(problems, endOffsets === startupRecordCount, `end offsets ${endOffsets} != expected ${startupRecordCount}`);
      summary = `produced ${producedRecords}, end offsets ${endOffsets} = expected`;
      break;
    }
  }
  if (problems.length > 0) {
    return { passed: false, details: problems.join('; ') };
  }
  return { passed: true, details: summary };
}

/** Waits until the broker's CPU use settles, so load from seeding or the previous run does not leak in. */
function coolDown(context: RunContext): Promise<void> {
  return waitForBrokerIdle(context.broker, {
    maxWaitMs: coolDownMaxWaitMs,
    cpuThresholdPercent: coolDownCpuThresholdPercent,
    log: context.log,
  });
}

async function measureRun(
  spec: RunSpec,
  context: RunContext,
  image: RuntimeImage,
  runClient: (options: ClientContainerOptions) => Promise<ClientContainerOutcome>,
): Promise<RunRecord> {
  const scenario = scenarios[spec.scenario];
  if (scenario.kind === 'consume') {
    await seedTopic(context.broker, {
      topic: spec.topic,
      payload: spec.payload,
      seed: spec.seed,
      records: spec.warmupCount + spec.messageCount,
      compression: scenario.seedCompression,
    });
    // The seeder is a JVM inside the broker container; its load must not overlap the measured run.
    await coolDown(context);
  }
  const startedAt = new Date().toISOString();
  const containerTimeoutMs = spec.timeoutMs + containerTimeoutGraceMs;
  const outcome = await runClient(
    clientContainerOptions({
      image,
      spec,
      broker: context.broker,
      budget: context.budget,
      timeoutMs: containerTimeoutMs,
    }),
  );
  const result = outcome.result ?? failedResult(spec, image, describeMissingResult(outcome, containerTimeoutMs));
  return {
    spec,
    result,
    verification: await verifyRun(spec, result, context.broker),
    image: { reference: image.reference, digest: image.imageId },
    startedAt,
    wallMs: Math.round(outcome.wallMs),
  };
}

/** Topic, optional seeding, client container, verification, topic deletion and cool-down for one spec. */
async function executeRun(spec: RunSpec, context: RunContext): Promise<RunAttempt> {
  const image = context.imageByRuntime.get(spec.runtime);
  const startedAt = new Date().toISOString();
  let infrastructureError: string | null;
  let record: RunRecord;
  try {
    if (image === undefined) {
      throw new Error(`no image was built for runtime ${spec.runtime}`);
    }
    const attempt = await context.attempts.run(
      { broker: context.broker, topic: spec.topic, partitions: spec.partitions },
      ({ runClient }) => measureRun(spec, context, image, runClient),
    );
    for (const error of attempt.cleanupErrors) context.log(error);
    infrastructureError = attempt.cleanupErrors[0] ?? null;
    if (!attempt.ok) throw attempt.error;
    record = attempt.value;
  } catch (error) {
    infrastructureError = errorMessage(error);
    const result = failedResult(spec, image, `harness error: ${infrastructureError}`);
    record = {
      spec,
      result,
      verification: { passed: false, details: `harness error: ${infrastructureError}` },
      image: { reference: image?.reference ?? '', digest: image?.imageId ?? null },
      startedAt,
      wallMs: 0,
    };
  }
  try {
    if (!context.isStopping()) await coolDown(context);
  } catch (error) {
    infrastructureError ??= `cool-down failed: ${errorMessage(error)}`;
  }
  return { record, infrastructureError };
}

export interface WarmUpOptions {
  runtimes: readonly RuntimeId[];
  payload: PayloadSize;
  runTag: string;
}

/** Warms the broker JVM's produce and fetch paths with throwaway smoke runs whose records are discarded. */
export async function warmUpBroker(context: RunContext, support: SupportMatrix, options: WarmUpOptions): Promise<void> {
  const candidates = support.cells.filter((cell) => cell.supported && options.runtimes.includes(cell.runtime));
  const cell = candidates.find((candidate) => candidate.client === 'kafka-core') ?? candidates[0];
  if (cell === undefined) {
    context.log('broker warm-up skipped: no supported cell');
    return;
  }
  for (const scenario of warmupScenarios) {
    const spec = createRunSpec({
      cell: { scenario, client: cell.client, series: 'warmup', runtime: cell.runtime, profile: 'normalized' },
      repetition: 0,
      runId: `${options.runTag}-warmup-${scenario.toLowerCase()}`,
      context: { tier: 'smoke', payload: options.payload, countScale: 1, brokers: [context.broker.bootstrap] },
    });
    const { record } = await executeRun(spec, context);
    const outcome = record.verification.passed ? 'ok' : `failed (${record.verification.details})`;
    context.log(`broker warm-up ${scenario} with ${cell.client} on ${cell.runtime}: ${outcome}`);
  }
}

function formatDuration(totalMs: number): string {
  const totalSeconds = Math.max(0, Math.round(totalMs / 1000));
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  if (hours > 0) {
    return `${hours}h${String(minutes).padStart(2, '0')}m`;
  }
  if (minutes > 0) {
    return `${minutes}m${String(seconds).padStart(2, '0')}s`;
  }
  return `${seconds}s`;
}

function describeRunOutcome(record: RunRecord): string {
  if (!record.verification.passed) {
    return `FAILED (${record.verification.details})`;
  }
  const metrics = record.result.metrics;
  if (metrics === null) {
    return 'ok';
  }
  if (metrics.startup !== null) {
    return `ok, first ack ${metrics.startup.firstAckedProduceMs.toFixed(0)} ms`;
  }
  if (record.spec.scenario === 'E1' && metrics.latency !== null) {
    return `ok, p99 ${metrics.latency.p99Ms.toFixed(2)} ms`;
  }
  return `ok, ${Math.round(metrics.recordsPerSecond).toLocaleString('en-US')} rec/s`;
}

/**
 * Runs every spec in order and writes each run record into the session. Stops early when
 * `context.isStopping()` turns true, and throws after repeated harness failures in a row.
 */
export async function executeSchedule(
  specs: readonly RunSpec[],
  context: RunContext,
  sessionDir: string,
): Promise<{ failedRuns: number }> {
  const loopStartedAtMs = performance.now();
  const total = specs.length;
  let failedRuns = 0;
  let consecutiveInfrastructureFailures = 0;
  for (const [index, spec] of specs.entries()) {
    if (context.isStopping()) {
      break;
    }
    const { record, infrastructureError } = await executeRun(spec, context);
    await writeJson(runRecordPath(sessionDir, spec.runId), record);
    if (!record.verification.passed) {
      failedRuns++;
    }
    const completed = index + 1;
    const etaMs = ((performance.now() - loopStartedAtMs) / completed) * (total - completed);
    const label = `${spec.scenario} ${spec.series} ${spec.runtime} ${spec.profile} r${spec.repetition}`;
    context.log(
      `[${String(completed).padStart(String(total).length)}/${total}] ${label}: ${describeRunOutcome(record)} | ETA ${formatDuration(etaMs)}`,
    );

    consecutiveInfrastructureFailures = infrastructureError === null ? 0 : consecutiveInfrastructureFailures + 1;
    if (consecutiveInfrastructureFailures >= maxConsecutiveInfrastructureFailures) {
      throw new Error(
        `Stopping: ${consecutiveInfrastructureFailures} harness failures in a row, last: ${infrastructureError}`,
      );
    }
  }
  return { failedRuns };
}
