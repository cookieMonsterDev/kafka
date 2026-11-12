import { spawn } from 'node:child_process';
import { StringDecoder } from 'node:string_decoder';
import { setTimeout as delay } from 'node:timers/promises';
import { resultLineMarker, resultSchemaVersion, runtimeIds, specEnvVar } from '../client/spec.ts';
import type { ClientResult, RunMetrics, RunSpec, RunStatus } from '../client/spec.ts';
import { runCommand } from './process.ts';
import type {
  BrokerHandle,
  ClientContainerOptions,
  ClientContainerOutcome,
  ResourceBudget,
  RuntimeImage,
} from './types.ts';

const outputTailBytes = 4096;
const maxResultLineChars = 1024 * 1024;
const clientPidsLimit = 1024;
const clientTmpfsBytes = 64 * 1024 * 1024;
const killGraceMs = 15_000;

/** Number of CPUs in a Docker cpuset such as `3-4` or `0-2,5`. */
export function countCpusetCpus(cpuset: string): number {
  let count = 0;
  for (const part of cpuset.split(',')) {
    const match = /^(\d+)(?:-(\d+))?$/.exec(part.trim());
    if (!match) {
      throw new Error(`Invalid cpuset "${cpuset}"`);
    }
    const first = Number(match[1]);
    const last = match[2] === undefined ? first : Number(match[2]);
    if (last < first) {
      throw new Error(`Invalid cpuset range "${part}" in "${cpuset}"`);
    }
    count += last - first + 1;
  }
  return count;
}

export function clientContainerName(runId: string): string {
  return `kafka-bench-client-${runId.replace(/[^a-zA-Z0-9_.-]/g, '-')}`;
}

/** Force-removes a container; succeeds when it does not exist. */
export async function removeContainer(name: string): Promise<void> {
  await runCommand('docker', ['rm', '--force', name], { allowFailure: true, timeoutMs: 30_000 });
}

export interface ClientRunTarget {
  image: RuntimeImage;
  spec: RunSpec;
  broker: BrokerHandle;
  budget: ResourceBudget;
  /** When the runner kills the container; give the client's own `spec.timeoutMs` some grace. */
  timeoutMs: number;
  args?: string[];
}

/** Container options for one client run on the broker's network, inside the budget's client cpuset and memory. */
export function clientContainerOptions(target: ClientRunTarget): ClientContainerOptions {
  return {
    image: target.image.reference,
    spec: target.spec,
    network: target.broker.network,
    cpuset: target.budget.clientCpuset,
    memoryBytes: target.budget.clientMemoryBytes,
    timeoutMs: target.timeoutMs,
    args: target.args,
  };
}

function runIdOf(spec: unknown): string {
  if (typeof spec === 'object' && spec !== null && 'runId' in spec && typeof spec.runId === 'string') {
    return spec.runId;
  }
  return `adhoc-${process.pid}-${Date.now()}`;
}

function buildDockerRunArgs(options: ClientContainerOptions, containerName: string): string[] {
  const memory = `${options.memoryBytes}b`;
  return [
    'run',
    '--rm',
    '--name',
    containerName,
    '--network',
    options.network,
    '--cpuset-cpus',
    options.cpuset,
    '--cpus',
    String(countCpusetCpus(options.cpuset)),
    '--memory',
    memory,
    '--memory-swap',
    memory,
    '--pids-limit',
    String(clientPidsLimit),
    '--cap-drop',
    'ALL',
    '--security-opt',
    'no-new-privileges',
    '--read-only',
    '--tmpfs',
    `/tmp:rw,nosuid,nodev,size=${clientTmpfsBytes}`,
    '--env',
    `${specEnvVar}=${JSON.stringify(options.spec)}`,
    options.image,
    ...(options.args ?? []),
  ];
}

/** Collects a stream's last bytes and the last complete line that starts with the result marker. */
class OutputCollector {
  tail = '';
  lastResultLine: string | null = null;
  private readonly decoder = new StringDecoder('utf8');
  private pendingLine = '';
  private isDiscardingLine = false;

  push(chunk: Buffer): void {
    const text = this.decoder.write(chunk);
    this.tail = (this.tail + text).slice(-outputTailBytes);
    this.consume(text);
  }

  end(): void {
    this.consume(this.decoder.end() + '\n');
  }

  private consume(text: string): void {
    const parts = (this.pendingLine + text).split('\n');
    this.pendingLine = parts.pop() ?? '';
    for (const line of parts) {
      if (this.isDiscardingLine) {
        this.isDiscardingLine = false;
        continue;
      }
      if (line.startsWith(resultLineMarker)) {
        this.lastResultLine = line;
      }
    }
    // A line this long is never a valid result; drop it instead of buffering unbounded output.
    if (this.pendingLine.length > maxResultLineChars) {
      this.pendingLine = '';
      this.isDiscardingLine = true;
    }
  }
}

/**
 * Runs one client in a fresh, resource-limited container and returns its parsed result line.
 * The container is killed when `timeoutMs` elapses; `result` is null when no valid line was printed.
 */
export async function runClientContainer(options: ClientContainerOptions): Promise<ClientContainerOutcome> {
  const containerName = clientContainerName(runIdOf(options.spec));
  const args = buildDockerRunArgs(options, containerName);
  const stdout = new OutputCollector();
  const stderr = new OutputCollector();
  const startedAtMs = performance.now();
  let isTimedOut = false;

  const child = spawn('docker', args, { stdio: ['ignore', 'pipe', 'pipe'] });
  child.stdout.on('data', (chunk: Buffer) => stdout.push(chunk));
  child.stderr.on('data', (chunk: Buffer) => stderr.push(chunk));

  const exited = new Promise<number | null>((resolve, reject) => {
    child.on('error', (error) => reject(new Error(`Cannot start docker run: ${error.message}`, { cause: error })));
    child.on('close', (exitCode) => resolve(exitCode));
  });

  const timer = setTimeout(() => {
    isTimedOut = true;
    void killAfterTimeout();
  }, options.timeoutMs);

  async function killAfterTimeout(): Promise<void> {
    await runCommand('docker', ['kill', containerName], { allowFailure: true, timeoutMs: 30_000 });
    await delay(killGraceMs, undefined, { ref: false });
    if (child.exitCode === null && child.signalCode === null) {
      child.kill('SIGKILL');
      await removeContainer(containerName);
    }
  }

  let exitCode: number | null;
  try {
    exitCode = await exited;
  } finally {
    clearTimeout(timer);
  }
  stdout.end();
  stderr.end();

  return {
    result: stdout.lastResultLine === null ? null : parseResultLine(stdout.lastResultLine),
    exitCode,
    timedOut: isTimedOut,
    wallMs: performance.now() - startedAtMs,
    stdoutTail: stdout.tail,
    stderrTail: stderr.tail,
  };
}

/** Parses one `resultLineMarker` line; null when the JSON or its shape is invalid. */
export function parseResultLine(line: string): ClientResult | null {
  if (!line.startsWith(resultLineMarker)) {
    return null;
  }
  let value: unknown;
  try {
    value = JSON.parse(line.slice(resultLineMarker.length));
  } catch {
    return null;
  }
  return isClientResult(value) ? value : null;
}

type UnknownRecord = Record<string, unknown>;

function isRecord(value: unknown): value is UnknownRecord {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isCount(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0;
}

function isNullableCount(value: unknown): boolean {
  return value === null || isCount(value);
}

function isStringOrNull(value: unknown): boolean {
  return value === null || typeof value === 'string';
}

const runStatuses: readonly RunStatus[] = ['ok', 'failed', 'unsupported'];
const metricCountKeys = [
  'records',
  'bytes',
  'elapsedMs',
  'recordsPerSecond',
  'megabytesPerSecond',
  'recordsPerCpuSecond',
  'rssBytes',
  'heapUsedBytes',
] as const satisfies readonly (keyof RunMetrics)[];
const metricNullableCountKeys = [
  'cgroupCpuSeconds',
  'peakMemoryBytes',
  'eventLoopDelayP99Ms',
] as const satisfies readonly (keyof RunMetrics)[];

function isLatencySummary(value: unknown): boolean {
  if (value === null) {
    return true;
  }
  if (!isRecord(value)) {
    return false;
  }
  return ['samples', 'p50Ms', 'p95Ms', 'p99Ms', 'maxMs'].every((key) => isCount(value[key]));
}

function isRunMetrics(value: unknown): boolean {
  if (!isRecord(value)) {
    return false;
  }
  if (!metricCountKeys.every((key) => isCount(value[key]))) {
    return false;
  }
  if (!metricNullableCountKeys.every((key) => isNullableCount(value[key]))) {
    return false;
  }
  const { cpu, gc, startup } = value;
  if (!isRecord(cpu) || !isCount(cpu.userSeconds) || !isCount(cpu.systemSeconds)) {
    return false;
  }
  if (gc !== null && (!isRecord(gc) || !isCount(gc.count) || !isCount(gc.totalMs))) {
    return false;
  }
  if (
    startup !== null &&
    (!isRecord(startup) || !isCount(startup.firstAckedProduceMs) || !isCount(startup.firstConsumedRecordMs))
  ) {
    return false;
  }
  return isLatencySummary(value.latency);
}

function isClientResult(value: unknown): value is ClientResult {
  if (!isRecord(value)) {
    return false;
  }
  const { runtime, observed, capabilityGaps } = value;
  if (value.schemaVersion !== resultSchemaVersion || typeof value.runId !== 'string') {
    return false;
  }
  if (!runStatuses.includes(value.status as RunStatus) || !isStringOrNull(value.error)) {
    return false;
  }
  if (!isRecord(runtime) || !runtimeIds.includes(runtime.id as (typeof runtimeIds)[number])) {
    return false;
  }
  if ((runtime.name !== 'node' && runtime.name !== 'bun') || typeof runtime.version !== 'string') {
    return false;
  }
  if (!isStringOrNull(value.clientVersion)) {
    return false;
  }
  if (!Array.isArray(capabilityGaps) || !capabilityGaps.every((gap) => typeof gap === 'string')) {
    return false;
  }
  if (!isRecord(observed)) {
    return false;
  }
  if (!isCount(observed.producedRecords) || !isCount(observed.consumedRecords) || !isCount(observed.consumedBytes)) {
    return false;
  }
  return value.metrics === null || isRunMetrics(value.metrics);
}
