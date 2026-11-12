import { docker } from './process.ts';
import type { ResourceBudget } from './types.ts';

const gibibyte = 1024 ** 3;
const mebibyte = 1024 ** 2;

export const minimumVmCpus = 6;
export const minimumVmMemoryBytes = 8 * gibibyte;
// `docker info` reports the VM's usable memory, which is a few hundred MiB below the Docker Desktop
// setting (10 GiB shows as about 9.7 GiB), so an 8 GiB setting must still pass.
const vmMemoryReportingSlackBytes = 512 * mebibyte;
const largeBudgetMinimumCpus = 8;

const brokerHeapBytes = 2 * gibibyte;
const brokerTmpfsBytes = 2 * gibibyte;
// tmpfs pages count against the container's memory limit, so the limit covers heap, tmpfs and JVM overhead.
const brokerMemoryBytes = 5 * gibibyte;
const clientMemoryBytes = 2 * gibibyte;

/** CPUs and memory of the Docker VM as `docker info` reports them. */
export interface DockerResources {
  cpus: number;
  memoryBytes: number;
}

export interface DockerInfo extends DockerResources {
  serverVersion: string;
  kernel: string;
  operatingSystem: string;
}

/** Describes why the Docker VM is too small for a session, or returns null when it is large enough. */
export function checkDockerResources(resources: DockerResources): string | null {
  const problems: string[] = [];
  if (resources.cpus < minimumVmCpus) {
    problems.push(`${resources.cpus} CPUs (need at least ${minimumVmCpus})`);
  }
  if (resources.memoryBytes < minimumVmMemoryBytes - vmMemoryReportingSlackBytes) {
    problems.push(
      `${formatGibibytes(resources.memoryBytes)} memory (need at least ${formatGibibytes(minimumVmMemoryBytes)})`,
    );
  }
  if (problems.length === 0) {
    return null;
  }
  return `Docker VM is too small: ${problems.join(', ')}. Raise it in Docker Desktop settings.`;
}

/**
 * Splits the Docker VM between the broker and the client under test, leaving the remaining vCPUs to
 * the VM kernel and dockerd. Memory limits are fixed so that results stay comparable across VM sizes.
 * Does not enforce the minimums: `doctor` reports them through `checkDockerResources`, so a `--force`
 * run on a smaller VM still gets a budget.
 */
export function deriveBudget(resources: DockerResources): ResourceBudget {
  const isLarge = resources.cpus >= largeBudgetMinimumCpus;
  return {
    vmCpus: resources.cpus,
    vmMemoryBytes: resources.memoryBytes,
    brokerCpuset: isLarge ? '0-3' : '0-2',
    brokerMemoryBytes,
    brokerHeapBytes,
    brokerTmpfsBytes,
    clientCpuset: isLarge ? '4-5' : '3-4',
    clientMemoryBytes,
  };
}

export function formatGibibytes(bytes: number): string {
  return `${(bytes / gibibyte).toFixed(1)} GiB`;
}

function readString(info: Record<string, unknown>, key: string): string {
  const value = info[key];
  if (typeof value !== 'string') {
    throw new Error(`docker info: expected string ${key}, got ${JSON.stringify(value)}`);
  }
  return value;
}

function readPositiveInteger(info: Record<string, unknown>, key: string): number {
  const value = info[key];
  if (typeof value !== 'number' || !Number.isInteger(value) || value <= 0) {
    throw new Error(`docker info: expected positive integer ${key}, got ${JSON.stringify(value)}`);
  }
  return value;
}

export async function readDockerInfo(): Promise<DockerInfo> {
  const stdout = await docker(['info', '--format', '{{json .}}'], { timeoutMs: 30_000 });
  let parsed: unknown;
  try {
    parsed = JSON.parse(stdout);
  } catch (error) {
    throw new Error(`docker info returned invalid JSON: ${stdout.slice(0, 200)}`, { cause: error });
  }
  if (typeof parsed !== 'object' || parsed === null) {
    throw new Error('docker info returned no object');
  }
  const info = parsed as Record<string, unknown>;
  return {
    serverVersion: readString(info, 'ServerVersion'),
    cpus: readPositiveInteger(info, 'NCPU'),
    memoryBytes: readPositiveInteger(info, 'MemTotal'),
    kernel: readString(info, 'KernelVersion'),
    operatingSystem: readString(info, 'OperatingSystem'),
  };
}
