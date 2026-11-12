// Host and Docker preflight checks. macOS-only checks (power, thermal) pass as no-ops elsewhere.

import { statfs } from 'node:fs/promises';
import { arch, cpus, loadavg, platform, release, totalmem } from 'node:os';
import { checkDockerResources, deriveBudget, formatGibibytes, readDockerInfo } from './budget.ts';
import { docker, errorMessage, runCommand } from './process.ts';
import { acquireSerialLock, LockUnavailableError } from './lock.ts';
import { repoRoot, sessionPaths, writeJson } from './session.ts';
import type { DoctorCheck, EnvironmentReport } from './types.ts';

const maxLoadAveragePerCpu = 0.5;
const minimumFreeDiskBytes = 20 * 1024 ** 3;
const benchmarkContainerPrefix = 'kafka-bench';
/** Images that ship `df`; whichever is present locally measures the VM disk without pulling anything. */
const diskProbeImagePatterns = ['apache/kafka:*', 'kafka-bench-client:*'];
const kibibyte = 1024;

export interface DoctorOptions {
  /** Downgrades failures to warnings. */
  force: boolean;
  /** When set, `environment.json` is written there. */
  sessionDir?: string;
  /** Receives the printed table; defaults to stdout. */
  print?: (text: string) => void;
}

type CheckOutcome = Omit<DoctorCheck, 'name'>;

const isMacOs = platform() === 'darwin';
const notMacOs: CheckOutcome = { status: 'pass', details: 'not macOS, check skipped' };

async function runPmset(args: readonly string[]): Promise<string | null> {
  try {
    const { stdout } = await runCommand('pmset', args, { timeoutMs: 10_000 });
    return stdout;
  } catch {
    return null;
  }
}

async function checkLowPowerMode(): Promise<CheckOutcome> {
  if (!isMacOs) {
    return notMacOs;
  }
  const output = await runPmset(['-g']);
  if (output === null) {
    return { status: 'warn', details: 'pmset -g failed; cannot tell whether Low Power Mode is on' };
  }
  const match = /^\s*lowpowermode\s+(\d+)/m.exec(output);
  if (!match) {
    return { status: 'pass', details: 'pmset reports no lowpowermode setting' };
  }
  if (match[1] !== '0') {
    return { status: 'fail', details: 'Low Power Mode is on; turn it off in System Settings > Battery' };
  }
  return { status: 'pass', details: 'off' };
}

async function checkAcPower(): Promise<CheckOutcome> {
  if (!isMacOs) {
    return notMacOs;
  }
  const output = await runPmset(['-g', 'batt']);
  if (output === null) {
    return { status: 'warn', details: 'pmset -g batt failed; cannot tell the power source' };
  }
  const match = /Now drawing from '([^']+)'/.exec(output);
  if (!match) {
    return { status: 'warn', details: 'pmset -g batt did not report a power source' };
  }
  if (match[1] !== 'AC Power') {
    return { status: 'fail', details: `running on ${match[1]}; connect the charger` };
  }
  return { status: 'pass', details: 'AC Power' };
}

async function checkThermal(): Promise<CheckOutcome> {
  if (!isMacOs) {
    return notMacOs;
  }
  const output = await runPmset(['-g', 'therm']);
  if (output === null) {
    return { status: 'warn', details: 'pmset -g therm failed' };
  }
  const pressureLines = output
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line !== '' && !line.startsWith('Note: No '))
    .filter((line) => {
      const speedLimit = /CPU_Speed_Limit\s*=\s*(\d+)/.exec(line);
      if (speedLimit) {
        return Number(speedLimit[1]) < 100;
      }
      return /warning level|speed limit|thermal/i.test(line);
    });
  if (pressureLines.length > 0) {
    return { status: 'warn', details: pressureLines.join('; ') };
  }
  return { status: 'pass', details: 'no thermal or performance warning recorded' };
}

async function checkOtherContainers(): Promise<CheckOutcome> {
  const stdout = await docker(['ps', '--format', '{{.Names}}'], { timeoutMs: 30_000 });
  const others = stdout
    .split('\n')
    .map((name) => name.trim())
    .filter((name) => name !== '' && !name.startsWith(benchmarkContainerPrefix));
  if (others.length > 0) {
    return { status: 'fail', details: `stop these first: ${others.join(', ')}` };
  }
  return { status: 'pass', details: 'none besides kafka-bench*' };
}

async function checkSerialLock(): Promise<CheckOutcome> {
  try {
    const lock = await acquireSerialLock({ waitTimeoutMs: 0 });
    const holder = lock.holder === 'self' ? 'this process' : 'a parent process';
    return { status: 'pass', details: `held by ${holder} (pid ${lock.ownerPid})` };
  } catch (error) {
    if (error instanceof LockUnavailableError) {
      return { status: 'fail', details: error.message };
    }
    throw error;
  }
}

function checkLoadAverage(): CheckOutcome {
  const [oneMinute = 0] = loadavg();
  const threshold = cpus().length * maxLoadAveragePerCpu;
  const details = `1-minute load ${oneMinute.toFixed(2)}, threshold ${threshold.toFixed(1)}`;
  return { status: oneMinute < threshold ? 'pass' : 'warn', details };
}

function freeDiskOutcome(freeBytes: number, totalBytes: number | null): CheckOutcome {
  const total = totalBytes === null ? '' : ` of ${formatGibibytes(totalBytes)}`;
  const details = `${formatGibibytes(freeBytes)} free${total}, want ${formatGibibytes(minimumFreeDiskBytes)}`;
  return { status: freeBytes >= minimumFreeDiskBytes ? 'pass' : 'warn', details };
}

/** Docker Desktop's disk image is a sparse file on the host, so the host needs room for it to grow. */
async function checkHostFreeDisk(): Promise<CheckOutcome> {
  const stats = await statfs(repoRoot);
  return freeDiskOutcome(stats.bavail * stats.bsize, stats.blocks * stats.bsize);
}

/** Size and free space of the last filesystem line of `df -kP` output, or null when it does not parse. */
function parseDfOutput(output: string): { totalBytes: number; freeBytes: number } | null {
  const lastLine = output.trim().split('\n').at(-1) ?? '';
  const [, totalKibibytes, , availableKibibytes] = lastLine.trim().split(/\s+/);
  if (totalKibibytes === undefined || availableKibibytes === undefined) {
    return null;
  }
  if (!/^\d+$/.test(totalKibibytes) || !/^\d+$/.test(availableKibibytes)) {
    return null;
  }
  return { totalBytes: Number(totalKibibytes) * kibibyte, freeBytes: Number(availableKibibytes) * kibibyte };
}

async function findLocalDiskProbeImage(): Promise<string | null> {
  const filters = diskProbeImagePatterns.flatMap((pattern) => ['--filter', `reference=${pattern}`]);
  const output = await docker(['image', 'ls', ...filters, '--format', '{{.Repository}}:{{.Tag}}'], {
    timeoutMs: 30_000,
  });
  const references = output.split('\n').map((line) => line.trim());
  return references.find((reference) => reference !== '' && !reference.includes('<none>')) ?? null;
}

/**
 * Images and build cache live on the Docker VM's disk, not on the host filesystem. A throwaway
 * container's root filesystem is an overlay on that disk, so its `df` reports the VM's free space.
 */
async function checkDockerFreeDisk(): Promise<CheckOutcome> {
  const unmeasured = (reason: string): CheckOutcome => ({
    status: 'warn',
    details: `could not measure the Docker VM disk: ${reason}`,
  });
  try {
    const image = await findLocalDiskProbeImage();
    if (image === null) {
      return unmeasured('no apache/kafka or kafka-bench-client image is present locally yet');
    }
    const output = await docker(
      [
        'run',
        '--rm',
        '--pull',
        'never',
        '--name',
        `${benchmarkContainerPrefix}-doctor-df-${process.pid}`,
        '--network',
        'none',
        '--entrypoint',
        'df',
        image,
        '-kP',
        '/',
      ],
      { timeoutMs: 60_000 },
    );
    const disk = parseDfOutput(output);
    if (disk === null) {
      return unmeasured(`unexpected df output from ${image}: ${output.trim().slice(0, 200)}`);
    }
    return freeDiskOutcome(disk.freeBytes, disk.totalBytes);
  } catch (error) {
    return unmeasured(errorMessage(error));
  }
}

function applyForce(check: DoctorCheck, force: boolean): DoctorCheck {
  if (!force || check.status !== 'fail') {
    return check;
  }
  return { ...check, status: 'warn', details: `${check.details} (forced)` };
}

/** Renders the checks as an aligned text table. */
export function formatDoctorReport(report: EnvironmentReport): string {
  const nameWidth = Math.max(5, ...report.checks.map((check) => check.name.length));
  const lines = [`${'Check'.padEnd(nameWidth)}  Status  Details`, `${'-'.repeat(nameWidth)}  ------  -------`];
  for (const check of report.checks) {
    lines.push(`${check.name.padEnd(nameWidth)}  ${check.status.toUpperCase().padEnd(6)}  ${check.details}`);
  }
  const { budget } = report;
  lines.push(
    '',
    `Budget: broker cpuset ${budget.brokerCpuset}, ${formatGibibytes(budget.brokerMemoryBytes)} ` +
      `(heap ${formatGibibytes(budget.brokerHeapBytes)}, tmpfs ${formatGibibytes(budget.brokerTmpfsBytes)}); ` +
      `client cpuset ${budget.clientCpuset}, ${formatGibibytes(budget.clientMemoryBytes)}`,
  );
  const failed = report.checks.filter((check) => check.status === 'fail').length;
  if (failed > 0) {
    lines.push(`Result: ${failed} check(s) failed. Fix them, or pass --force for a development run.`);
  } else if (report.warnings.length > 0) {
    lines.push(`Result: usable, with warnings (${report.warnings.join(', ')}); this session cannot be published.`);
  } else {
    lines.push('Result: all checks passed.');
  }
  return lines.join('\n');
}

export function hasFailedChecks(report: EnvironmentReport): boolean {
  return report.checks.some((check) => check.status === 'fail');
}

/**
 * Runs every preflight check, prints the table and returns the report (also written to
 * `<sessionDir>/environment.json` when given). Takes the serial lock without waiting if this
 * process does not hold it yet. Throws only when Docker itself is unreachable.
 */
export async function runDoctor(options: DoctorOptions): Promise<EnvironmentReport> {
  const takenAt = new Date().toISOString();
  const lockOutcome = await checkSerialLock();
  const dockerInfo = await readDockerInfo();
  const dockerProblem = checkDockerResources(dockerInfo);
  const dockerOutcome: CheckOutcome =
    dockerProblem === null
      ? { status: 'pass', details: `${dockerInfo.cpus} CPUs, ${formatGibibytes(dockerInfo.memoryBytes)}` }
      : { status: 'fail', details: dockerProblem };

  const rawChecks: DoctorCheck[] = [
    { name: 'low-power-mode', ...(await checkLowPowerMode()) },
    { name: 'ac-power', ...(await checkAcPower()) },
    { name: 'docker-vm-size', ...dockerOutcome },
    { name: 'no-other-containers', ...(await checkOtherContainers()) },
    { name: 'serial-lock', ...lockOutcome },
    { name: 'load-average', ...checkLoadAverage() },
    { name: 'thermal', ...(await checkThermal()) },
    { name: 'docker-free-disk', ...(await checkDockerFreeDisk()) },
    { name: 'host-free-disk', ...(await checkHostFreeDisk()) },
  ];
  const checks = rawChecks.map((check) => applyForce(check, options.force));
  const firstCpu = cpus()[0];
  const report: EnvironmentReport = {
    takenAt,
    host: {
      platform: platform(),
      arch: arch(),
      cpuModel: firstCpu?.model ?? 'unknown',
      cpuCount: cpus().length,
      memoryBytes: totalmem(),
      osRelease: release(),
    },
    docker: {
      serverVersion: dockerInfo.serverVersion,
      vmCpus: dockerInfo.cpus,
      vmMemoryBytes: dockerInfo.memoryBytes,
      kernel: dockerInfo.kernel,
    },
    budget: deriveBudget(dockerInfo),
    checks,
    forced: options.force,
    warnings: checks.filter((check) => check.status === 'warn').map((check) => check.name),
  };

  (options.print ?? ((text: string) => process.stdout.write(`${text}\n`)))(formatDoctorReport(report));
  if (options.sessionDir !== undefined) {
    await writeJson(sessionPaths(options.sessionDir).environment, report);
  }
  return report;
}
