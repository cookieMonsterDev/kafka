// Broker lifecycle and admin operations. The broker sits on an internal network with no published
// ports, so every admin command runs through `docker exec` inside the broker container.

import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { createSeedPayloadFile } from '../client/payload.ts';
import type { PayloadSize } from '../client/spec.ts';
import { parseRepoDigest, repoDigestsInspectArgs } from './images.ts';
import { runCommand, tailLines } from './process.ts';
import type { CommandResult } from './process.ts';
import type { BrokerHandle, ResourceBudget } from './types.ts';

const composeFile = fileURLToPath(new URL('../docker/compose.yml', import.meta.url));
const composeProject = 'kafka-bench';
const brokerContainerName = 'kafka-bench-broker';
const brokerNetwork = 'kafka-bench';
const bootstrap = 'broker:9092';
const toolsDir = '/opt/kafka/bin';
const dataDir = '/var/lib/kafka/data';
// The container's KAFKA_HEAP_OPTS (the broker's full heap) is inherited by `docker exec`; admin tools
// must not reserve that much inside the broker's memory limit.
const toolHeapOptions = '-Xms64m -Xmx512m';
const readyTimeoutMs = 180_000;
const topicReadyTimeoutMs = 30_000;
const topicGoneTimeoutMs = 60_000;
const pollIntervalMs = 1000;
const toolTimeoutMs = 120_000;
const minimumSeedRecordsPerSecond = 10_000;
const cpuSampleWindowMs = 1000;
const idleSamplesRequired = 2;

function composeEnvironment(budget: ResourceBudget): NodeJS.ProcessEnv {
  return {
    ...process.env,
    BROKER_CPUSET: budget.brokerCpuset,
    BROKER_MEMORY_BYTES: String(budget.brokerMemoryBytes),
    BROKER_HEAP_BYTES: String(budget.brokerHeapBytes),
    BROKER_TMPFS_BYTES: String(budget.brokerTmpfsBytes),
  };
}

// Compose interpolates the whole file even for `down`, so the required variables need some value.
const placeholderEnvironment: NodeJS.ProcessEnv = {
  ...process.env,
  BROKER_CPUSET: '0',
  BROKER_MEMORY_BYTES: '0',
  BROKER_HEAP_BYTES: '0',
  BROKER_TMPFS_BYTES: '0',
};

function compose(args: readonly string[], env: NodeJS.ProcessEnv, timeoutMs: number): Promise<CommandResult> {
  return runCommand('docker', ['compose', '--project-name', composeProject, '--file', composeFile, ...args], {
    env,
    timeoutMs,
  });
}

interface ToolOptions {
  input?: string;
  timeoutMs?: number;
  allowFailure?: boolean;
}

function execInBroker(
  broker: BrokerHandle,
  command: readonly string[],
  options: ToolOptions = {},
): Promise<CommandResult> {
  const interactive = options.input === undefined ? [] : ['--interactive'];
  return runCommand(
    'docker',
    ['exec', ...interactive, '--env', `KAFKA_HEAP_OPTS=${toolHeapOptions}`, broker.containerName, ...command],
    { input: options.input, timeoutMs: options.timeoutMs ?? toolTimeoutMs, allowFailure: options.allowFailure },
  );
}

function kafkaTool(broker: BrokerHandle, tool: string, args: readonly string[], options: ToolOptions = {}) {
  return execInBroker(broker, [`${toolsDir}/${tool}`, '--bootstrap-server', broker.bootstrap, ...args], options);
}

async function inspectContainer(name: string, format: string): Promise<string | null> {
  const result = await runCommand('docker', ['inspect', '--format', format, name], {
    allowFailure: true,
    timeoutMs: 30_000,
  });
  return result.exitCode === 0 ? result.stdout.trim() : null;
}

async function readImageDigest(image: string): Promise<string | null> {
  const result = await runCommand('docker', repoDigestsInspectArgs(image), { allowFailure: true, timeoutMs: 30_000 });
  if (result.exitCode !== 0) {
    return null;
  }
  return parseRepoDigest(image, result.stdout);
}

async function waitUntilReady(broker: BrokerHandle): Promise<void> {
  const deadlineMs = Date.now() + readyTimeoutMs;
  let lastError = '';
  while (Date.now() < deadlineMs) {
    const state = await inspectContainer(broker.containerName, '{{.State.Status}}');
    if (state !== 'running') {
      const logs = await runCommand('docker', ['logs', '--tail', '40', broker.containerName], {
        allowFailure: true,
        timeoutMs: 30_000,
      });
      throw new Error(`Broker container is ${state ?? 'missing'}:\n${logs.stdout}${logs.stderr}`);
    }
    const result = await kafkaTool(broker, 'kafka-topics.sh', ['--list'], { allowFailure: true, timeoutMs: 60_000 });
    if (result.exitCode === 0) {
      return;
    }
    lastError = tailLines(result.stderr, 3).replaceAll('\n', ' ');
    await delay(pollIntervalMs);
  }
  throw new Error(`Broker was not ready within ${readyTimeoutMs} ms: ${lastError}`);
}

/**
 * Starts (or reuses) the single-node KRaft broker pinned to the budget's cpuset and memory limit,
 * and resolves once it answers admin requests. Compose leaves a running broker with an unchanged
 * configuration in place, so this is idempotent.
 */
export async function startBroker(budget: ResourceBudget): Promise<BrokerHandle> {
  await compose(['up', '--detach', '--quiet-pull'], composeEnvironment(budget), 600_000);
  const image = await inspectContainer(brokerContainerName, '{{.Config.Image}}');
  if (image === null) {
    throw new Error(`docker compose did not create ${brokerContainerName}`);
  }
  const broker: BrokerHandle = {
    containerName: brokerContainerName,
    network: brokerNetwork,
    bootstrap,
    image,
    imageDigest: await readImageDigest(image),
  };
  await waitUntilReady(broker);
  return broker;
}

/** Stops the broker and removes its container and network. */
export async function stopBroker(broker: BrokerHandle): Promise<void> {
  await compose(['down', '--remove-orphans', '--timeout', '10'], placeholderEnvironment, 120_000);
  const leftover = await inspectContainer(broker.containerName, '{{.State.Status}}');
  if (leftover !== null) {
    throw new Error(`${broker.containerName} is still present (${leftover}) after docker compose down`);
  }
}

function countPartitionsWithLeader(describeOutput: string, topic: string): number {
  let count = 0;
  for (const line of describeOutput.split('\n')) {
    const match = /\bTopic:\s*(\S+)\s+Partition:\s*\d+\s+Leader:\s*(-?\d+)/.exec(line);
    if (match && match[1] === topic && Number(match[2]) >= 0) {
      count++;
    }
  }
  return count;
}

/** Creates a topic with replication factor 1 and waits until every partition has a leader. */
export async function createTopic(broker: BrokerHandle, topic: string, partitions: number): Promise<void> {
  await kafkaTool(broker, 'kafka-topics.sh', [
    '--create',
    '--topic',
    topic,
    '--partitions',
    String(partitions),
    '--replication-factor',
    '1',
  ]);
  const deadlineMs = Date.now() + topicReadyTimeoutMs;
  while (Date.now() < deadlineMs) {
    const result = await kafkaTool(broker, 'kafka-topics.sh', ['--describe', '--topic', topic], { allowFailure: true });
    if (result.exitCode === 0 && countPartitionsWithLeader(result.stdout, topic) === partitions) {
      return;
    }
    await delay(pollIntervalMs);
  }
  throw new Error(`Topic ${topic} did not get leaders for ${partitions} partitions within ${topicReadyTimeoutMs} ms`);
}

async function listTopicDataDirs(broker: BrokerHandle, topic: string): Promise<string[]> {
  const { stdout } = await execInBroker(broker, ['ls', '-1', dataDir], { timeoutMs: 30_000 });
  const prefix = `${topic}-`;
  // Partition dirs are `<topic>-<n>`; deleted ones are renamed to `<topic>-<n>.<id>-delete` until removed.
  return stdout.split('\n').filter((name) => name.startsWith(prefix) && /^\d+(\.|$)/.test(name.slice(prefix.length)));
}

/** Deletes a topic and waits until its partition directories are gone from the broker's tmpfs. */
export async function deleteTopic(broker: BrokerHandle, topic: string): Promise<void> {
  await kafkaTool(broker, 'kafka-topics.sh', ['--delete', '--if-exists', '--topic', topic]);
  const deadlineMs = Date.now() + topicGoneTimeoutMs;
  let remaining: string[] = [];
  while (Date.now() < deadlineMs) {
    remaining = await listTopicDataDirs(broker, topic);
    if (remaining.length === 0) {
      return;
    }
    await delay(pollIntervalMs);
  }
  throw new Error(`Topic ${topic} still has data after ${topicGoneTimeoutMs} ms: ${remaining.join(', ')}`);
}

/** Sum of the latest offsets over all partitions of `topic`. */
export async function readEndOffsetTotal(broker: BrokerHandle, topic: string): Promise<number> {
  const { stdout } = await kafkaTool(broker, 'kafka-get-offsets.sh', ['--topic', topic, '--time', '-1']);
  let total = 0;
  let partitionCount = 0;
  for (const line of stdout.split('\n')) {
    const match = /^(.+):(\d+):(\d+)$/.exec(line.trim());
    if (!match || match[1] !== topic) {
      continue;
    }
    total += Number(match[3]);
    partitionCount++;
  }
  if (partitionCount === 0) {
    throw new Error(`kafka-get-offsets.sh returned no partitions for ${topic}: ${stdout.trim().slice(0, 300)}`);
  }
  return total;
}

export interface SeedTopicOptions {
  topic: string;
  payload: PayloadSize;
  seed: number;
  records: number;
  compression: 'none' | 'gzip';
}

/**
 * Fills a topic with `records` values using Kafka's own `kafka-producer-perf-test.sh`, so no client
 * under test writes the data another one reads. Values come from the seeded payload file; records
 * have no keys or headers. Verifies the end offsets afterwards.
 */
export async function seedTopic(broker: BrokerHandle, options: SeedTopicOptions): Promise<void> {
  const payloadPath = `/tmp/kafka-bench-payload-${options.payload}-${options.seed}.txt`;
  await execInBroker(broker, ['sh', '-c', 'cat > "$1"', 'write-payload', payloadPath], {
    input: createSeedPayloadFile(options.payload, options.seed),
    timeoutMs: 30_000,
  });
  const timeoutMs = Math.max(toolTimeoutMs, Math.ceil((options.records / minimumSeedRecordsPerSecond) * 1000));
  await kafkaTool(
    broker,
    'kafka-producer-perf-test.sh',
    [
      '--topic',
      options.topic,
      '--num-records',
      String(options.records),
      '--throughput',
      '-1',
      '--payload-file',
      payloadPath,
      '--command-property',
      'acks=all',
      `compression.type=${options.compression}`,
      'linger.ms=10',
      'batch.size=262144',
      'client.id=kafka-bench-seeder',
    ],
    { timeoutMs },
  );
  const endOffsetTotal = await readEndOffsetTotal(broker, options.topic);
  if (endOffsetTotal !== options.records) {
    throw new Error(`Seeding ${options.topic} wrote ${endOffsetTotal} records, expected ${options.records}`);
  }
}

async function readBrokerCpuUsageMicros(broker: BrokerHandle): Promise<number> {
  const { stdout } = await execInBroker(broker, ['cat', '/sys/fs/cgroup/cpu.stat'], { timeoutMs: 30_000 });
  const match = /^usage_usec (\d+)$/m.exec(stdout);
  if (!match) {
    throw new Error(`Cannot read usage_usec from the broker's cpu.stat: ${stdout.slice(0, 200)}`);
  }
  return Number(match[1]);
}

export interface WaitForBrokerIdleOptions {
  maxWaitMs: number;
  /** Idle when CPU use stays below this, in percent of one vCPU, for consecutive one-second windows. */
  cpuThresholdPercent: number;
  log?: (line: string) => void;
}

/**
 * Cool-down between runs: waits until the broker container's cgroup CPU usage stays below the
 * threshold. Returns after `maxWaitMs` even when the broker never went idle (and logs that).
 */
export async function waitForBrokerIdle(broker: BrokerHandle, options: WaitForBrokerIdleOptions): Promise<void> {
  const deadlineMs = Date.now() + options.maxWaitMs;
  let idleSamples = 0;
  let lastPercent = Number.NaN;
  let previousUsageMicros = await readBrokerCpuUsageMicros(broker);
  let previousAtMs = performance.now();
  while (Date.now() < deadlineMs) {
    await delay(cpuSampleWindowMs);
    const usageMicros = await readBrokerCpuUsageMicros(broker);
    const nowMs = performance.now();
    lastPercent = ((usageMicros - previousUsageMicros) / 1000 / (nowMs - previousAtMs)) * 100;
    previousUsageMicros = usageMicros;
    previousAtMs = nowMs;
    idleSamples = lastPercent < options.cpuThresholdPercent ? idleSamples + 1 : 0;
    if (idleSamples >= idleSamplesRequired) {
      return;
    }
  }
  options.log?.(
    `[broker] not idle after ${options.maxWaitMs} ms (last ${lastPercent.toFixed(1)}% CPU, ` +
      `threshold ${options.cpuThresholdPercent}%); continuing`,
  );
}
