// The machine-wide lock shared with ~/.cache/kafka-bun/serial.sh: a directory created with mkdir
// (atomic), holding the owner's pid. Keep the protocol in step with that script.

import { execFileSync } from 'node:child_process';
import { mkdirSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

export const serialLockDir = '/tmp/kafka-serial.lock';
const pidFile = join(serialLockDir, 'pid');
const defaultPollMs = 2000;
const waitLogIntervalMs = 30_000;
// serial.sh writes the pid right after mkdir; a lock without a pid file for longer than this was
// left behind by an owner that died in between.
const ownerlessLockStaleMs = 30_000;

/** Exit status of a process ended by each handled signal: 128 plus the signal number. */
export const signalExitCodes = { SIGHUP: 129, SIGINT: 130, SIGTERM: 143 } as const;
export type HandledSignal = keyof typeof signalExitCodes;
const handledSignals = Object.keys(signalExitCodes) as HandledSignal[];

export interface SerialLock {
  /** `self` when this process created the lock; `ancestor` when a parent such as serial.sh holds it. */
  holder: 'self' | 'ancestor';
  ownerPid: number;
  /** Removes the lock if this process created it; a no-op for an ancestor's lock. */
  release(): void;
}

export interface AcquireLockOptions {
  /** How long to wait for another owner; 0 tries once. Defaults to waiting indefinitely. */
  waitTimeoutMs?: number;
  pollMs?: number;
  /** Release and exit on SIGINT/SIGTERM/SIGHUP. Disable when the caller runs its own cleanup first. */
  handleSignals?: boolean;
  log?: (line: string) => void;
}

export class LockUnavailableError extends Error {
  readonly ownerPid: number | null;

  constructor(message: string, ownerPid: number | null) {
    super(message);
    this.name = 'LockUnavailableError';
    this.ownerPid = ownerPid;
  }
}

let heldLock: SerialLock | null = null;
let ancestorPids: Set<number> | null = null;

function parentPidOf(pid: number): number | null {
  try {
    const output = execFileSync('ps', ['-o', 'ppid=', '-p', String(pid)], { encoding: 'utf8' }).trim();
    const parentPid = Number(output);
    return Number.isInteger(parentPid) && parentPid > 0 ? parentPid : null;
  } catch {
    return null;
  }
}

/** This process and every ancestor up to init. */
function processAndAncestorPids(): Set<number> {
  if (ancestorPids !== null) {
    return ancestorPids;
  }
  const pids = new Set<number>([process.pid]);
  let currentPid: number | null = process.ppid;
  while (currentPid !== null && currentPid > 1 && !pids.has(currentPid)) {
    pids.add(currentPid);
    currentPid = parentPidOf(currentPid);
  }
  ancestorPids = pids;
  return pids;
}

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === 'EPERM';
  }
}

function readOwnerPid(): number | null {
  try {
    const pid = Number(readFileSync(pidFile, 'utf8').trim());
    return Number.isInteger(pid) && pid > 0 ? pid : null;
  } catch {
    return null;
  }
}

function lockAgeMs(): number | null {
  try {
    return Date.now() - statSync(serialLockDir).mtimeMs;
  } catch {
    return null;
  }
}

function tryCreateLock(): boolean {
  try {
    mkdirSync(serialLockDir);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'EEXIST') {
      return false;
    }
    throw error;
  }
  writeFileSync(pidFile, `${process.pid}\n`);
  return true;
}

function createOwnedLock(handleSignals: boolean): SerialLock {
  const releaseOnSignal = (signal: HandledSignal): void => {
    lock.release();
    process.exit(signalExitCodes[signal]);
  };
  const releaseOnExit = (): void => lock.release();
  const lock: SerialLock = {
    holder: 'self',
    ownerPid: process.pid,
    release() {
      if (heldLock !== lock) {
        return;
      }
      heldLock = null;
      process.off('exit', releaseOnExit);
      for (const signal of handledSignals) {
        process.off(signal, releaseOnSignal);
      }
      if (readOwnerPid() === process.pid) {
        rmSync(serialLockDir, { recursive: true, force: true });
      }
    },
  };
  process.on('exit', releaseOnExit);
  if (handleSignals) {
    for (const signal of handledSignals) {
      process.on(signal, releaseOnSignal);
    }
  }
  return lock;
}

/**
 * Acquires the machine-wide serial lock. Re-entrant: when this process or one of its ancestors
 * (for example `serial.sh` wrapping the CLI) already holds it, resolves immediately. Steals a lock
 * whose owner is dead. Throws `LockUnavailableError` when `waitTimeoutMs` elapses.
 */
export async function acquireSerialLock(options: AcquireLockOptions = {}): Promise<SerialLock> {
  if (heldLock !== null) {
    return heldLock;
  }
  const waitTimeoutMs = options.waitTimeoutMs ?? Number.POSITIVE_INFINITY;
  const pollMs = options.pollMs ?? defaultPollMs;
  const startedAtMs = Date.now();
  let lastLogAtMs = startedAtMs;

  while (true) {
    if (tryCreateLock()) {
      heldLock = createOwnedLock(options.handleSignals ?? true);
      return heldLock;
    }
    const ownerPid = readOwnerPid();
    if (ownerPid !== null && processAndAncestorPids().has(ownerPid)) {
      heldLock = { holder: 'ancestor', ownerPid, release() {} };
      return heldLock;
    }
    if (ownerPid !== null && !isProcessAlive(ownerPid)) {
      options.log?.(`[lock] removing stale lock of dead pid ${ownerPid}`);
      rmSync(serialLockDir, { recursive: true, force: true });
      continue;
    }
    const ageMs = lockAgeMs();
    if (ownerPid === null && ageMs !== null && ageMs > ownerlessLockStaleMs) {
      options.log?.('[lock] removing stale lock without an owner pid');
      rmSync(serialLockDir, { recursive: true, force: true });
      continue;
    }

    const waitedMs = Date.now() - startedAtMs;
    if (waitedMs >= waitTimeoutMs) {
      const owner = ownerPid === null ? 'an unknown process' : `pid ${ownerPid}`;
      throw new LockUnavailableError(`${serialLockDir} is held by ${owner}`, ownerPid);
    }
    if (Date.now() - lastLogAtMs >= waitLogIntervalMs) {
      lastLogAtMs = Date.now();
      options.log?.(
        `[lock] waiting for ${serialLockDir} (pid ${ownerPid ?? '?'}), ${Math.round(waitedMs / 1000)} s so far`,
      );
    }
    await delay(Math.min(pollMs, Math.max(0, waitTimeoutMs - waitedMs)));
  }
}
