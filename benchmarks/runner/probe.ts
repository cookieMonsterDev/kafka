import { randomUUID } from 'node:crypto';
import { mkdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';

import { payloadSeed, scenarios } from '../client/spec.ts';
import type { ClientId, RunSpec, RuntimeId } from '../client/spec.ts';
import type { BenchmarkAttempts } from './attempt.ts';
import { clientContainerOptions } from './container.ts';
import { writeJson } from './session.ts';
import type {
  BrokerHandle,
  ClientContainerOutcome,
  ResourceBudget,
  RuntimeImage,
  SupportCell,
  SupportMatrix,
} from './types.ts';

export interface ProbeOptions {
  attempts: BenchmarkAttempts;
  images: readonly RuntimeImage[];
  clients: readonly ClientId[];
  broker: BrokerHandle;
  budget: ResourceBudget;
  /** Directory for per-image probe results, reused while the image id stays the same. */
  cacheDir: string;
  log: (line: string) => void;
}

const probeTopicPartitions = 1;
/** Deadline inside the client: import, connect, produce one, consume one. */
const probeClientTimeoutMs = 60_000;
/** The runner kills the container a little after the client's own deadline. */
const probeContainerTimeoutMs = probeClientTimeoutMs + 30_000;
const maxReasonLength = 300;
const notInstalledReason =
  "package is not installed in the image: its install failed for this runtime's Node ABI (see the image build log)";

interface CachedCell {
  supported: boolean;
  reason: string;
}

interface ProbeCacheFile {
  imageId: string;
  cells: Partial<Record<ClientId, CachedCell>>;
}

function cacheFilePath(cacheDir: string, imageId: string): string {
  const safeId = imageId.replace(/^sha256:/, '').replace(/[^0-9a-zA-Z]/g, '');
  return join(cacheDir, `probe-${safeId}.json`);
}

function isCachedCell(value: unknown): value is CachedCell {
  if (typeof value !== 'object' || value === null) return false;
  const cell = value as Record<string, unknown>;
  return typeof cell.supported === 'boolean' && typeof cell.reason === 'string';
}

async function readCache(cacheDir: string, imageId: string): Promise<ProbeCacheFile> {
  const empty: ProbeCacheFile = { imageId, cells: {} };
  let text: string;
  try {
    text = await readFile(cacheFilePath(cacheDir, imageId), 'utf8');
  } catch {
    return empty;
  }

  try {
    const parsed = JSON.parse(text) as { imageId?: unknown; cells?: unknown };
    if (parsed.imageId !== imageId || typeof parsed.cells !== 'object' || parsed.cells === null) return empty;
    const cells: ProbeCacheFile['cells'] = {};
    for (const [client, cell] of Object.entries(parsed.cells)) {
      if (isCachedCell(cell)) cells[client as ClientId] = { supported: cell.supported, reason: cell.reason };
    }
    return { imageId, cells };
  } catch {
    return empty;
  }
}

async function writeCache(cacheDir: string, cache: ProbeCacheFile): Promise<void> {
  await mkdir(cacheDir, { recursive: true });
  await writeJson(cacheFilePath(cacheDir, cache.imageId), cache);
}

function probeSpec(client: ClientId, runtime: RuntimeId, broker: BrokerHandle, topic: string, runId: string): RunSpec {
  const scenario = scenarios.P1;
  return {
    runId,
    scenario: scenario.id,
    client,
    runtime,
    profile: 'normalized',
    series: client,
    repetition: 0,
    brokers: [broker.bootstrap],
    topic,
    partitions: probeTopicPartitions,
    groupId: `${topic}-group`,
    payload: 'small',
    seed: payloadSeed,
    messageCount: 1,
    warmupCount: 0,
    rateMessagesPerSecond: 0,
    durationMs: 0,
    wire: scenario.wire,
    timeoutMs: probeClientTimeoutMs,
  };
}

/** Stack frames, carets, source excerpts and Node's own noise say nothing about why a cell failed. */
function isNoiseLine(line: string): boolean {
  return (
    line.length === 0 ||
    /^at\s/.test(line) ||
    /^\^+$/.test(line) ||
    /^node:internal\//.test(line) ||
    /^Node\.js v\d/.test(line) ||
    /^Bun v\d/.test(line) ||
    /^\(Use `node --trace-/.test(line) ||
    /ExperimentalWarning/.test(line) ||
    /^\d+ \|/.test(line) ||
    /^[{}[\]]+,?$/.test(line)
  );
}

const errorLinePattern =
  /error|cannot|can't|could not|not found|failed|unsupported|undefined symbol|segmentation|abort|ERR_[A-Z_]+|ENOENT|ECONNREFUSED/i;

/** The first line that reads like an error, else the first non-noise line. */
export function firstMeaningfulLine(text: string): string | null {
  const lines = text
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => !isNoiseLine(line));
  const line = lines.find((candidate) => errorLinePattern.test(candidate)) ?? lines[0];
  if (line === undefined) return null;
  return line.length > maxReasonLength ? `${line.slice(0, maxReasonLength - 3)}...` : line;
}

function failureReason(outcome: ClientContainerOutcome): string {
  const { result } = outcome;
  if (result !== null) {
    const fromError = result.error === null ? null : firstMeaningfulLine(result.error);
    return fromError ?? `client reported status ${result.status} without an error`;
  }
  if (outcome.timedOut) {
    const fromStderr = firstMeaningfulLine(outcome.stderrTail);
    const timeoutSeconds = Math.round(probeContainerTimeoutMs / 1000);
    return fromStderr === null
      ? `timed out after ${timeoutSeconds}s`
      : `timed out after ${timeoutSeconds}s: ${fromStderr}`;
  }
  const fromOutput = firstMeaningfulLine(outcome.stderrTail) ?? firstMeaningfulLine(outcome.stdoutTail);
  const exit = `exit code ${String(outcome.exitCode)}, no result line`;
  return fromOutput === null ? exit : `${fromOutput} (${exit})`;
}

interface CellOutcome {
  cell: CachedCell;
  /** Timeouts can be transient (a slow broker), so they are not cached. */
  cacheable: boolean;
}

async function probeCell(
  image: RuntimeImage,
  client: ClientId,
  broker: BrokerHandle,
  budget: ResourceBudget,
  attempts: BenchmarkAttempts,
  log: (line: string) => void,
): Promise<CellOutcome> {
  if (image.clientVersions[client] === null) {
    return { cell: { supported: false, reason: notInstalledReason }, cacheable: true };
  }

  const runId = `probe-${client}-${image.runtime}-${randomUUID().slice(0, 8)}`;
  const topic = `kafka-bench-${runId}`;
  const attempt = await attempts.run({ broker, topic, partitions: probeTopicPartitions }, async ({ runClient }) => {
    const outcome = await runClient(
      clientContainerOptions({
        image,
        spec: probeSpec(client, image.runtime, broker, topic, runId),
        broker,
        budget,
        timeoutMs: probeContainerTimeoutMs,
        args: ['probe'],
      }),
    );
    if (outcome.result?.status === 'ok') return { cell: { supported: true, reason: '' }, cacheable: true };
    return { cell: { supported: false, reason: failureReason(outcome) }, cacheable: !outcome.timedOut };
  });
  for (const error of attempt.cleanupErrors) log(`probe: ${error}`);
  if (!attempt.ok) throw attempt.error;
  return attempt.value;
}

/**
 * Runs import, connect, produce one and consume one for every (client, runtime) cell. Results are
 * cached per image id in `cacheDir`, so an unchanged image is not probed again.
 */
export async function probeSupport(options: ProbeOptions): Promise<SupportMatrix> {
  const { images, clients, broker, budget, cacheDir, log } = options;
  const imageIds: Partial<Record<RuntimeId, string>> = {};
  const cells: SupportCell[] = [];

  for (const image of images) {
    imageIds[image.runtime] = image.imageId;
    const cache = await readCache(cacheDir, image.imageId);

    for (const client of clients) {
      const cached = cache.cells[client];
      if (cached !== undefined) {
        log(`probe ${client} on ${image.runtime}: ${cached.supported ? 'supported' : cached.reason} (cached)`);
        cells.push({ client, runtime: image.runtime, ...cached });
        continue;
      }

      const { cell, cacheable } = await probeCell(image, client, broker, budget, options.attempts, log);
      log(`probe ${client} on ${image.runtime}: ${cell.supported ? 'supported' : cell.reason}`);
      cells.push({ client, runtime: image.runtime, ...cell });
      if (!cacheable) continue;
      cache.cells[client] = cell;
      await writeCache(cacheDir, cache);
    }
  }

  return { imageIds, cells };
}
