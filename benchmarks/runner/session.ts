import { mkdir, readdir, readFile, rename, stat, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

export const benchmarksRoot = fileURLToPath(new URL('..', import.meta.url));
export const repoRoot = join(benchmarksRoot, '..');
export const rawResultsDir = join(benchmarksRoot, 'results', 'raw');
export const publishedResultsDir = join(benchmarksRoot, 'results', 'published');
/** Probe results cached per image id; not a session, so `latestSessionDir` skips it. */
export const probeCacheDir = join(rawResultsDir, 'probe-cache');

const sessionIdPattern = /^\d{14}-[0-9a-f]{12}$/;

/** `<UTC yyyymmddHHMMSS>-<first 12 chars of the core SHA>`, from the clock at call time. */
export function createSessionId(coreSha: string, now: Date = new Date()): string {
  if (!/^[0-9a-f]{12,}$/.test(coreSha)) {
    throw new Error(`Invalid core SHA "${coreSha}"`);
  }
  const timestamp = now.toISOString().replace(/\D/g, '').slice(0, 14);
  return `${timestamp}-${coreSha.slice(0, 12)}`;
}

export interface SessionPaths {
  dir: string;
  session: string;
  environment: string;
  supportMatrix: string;
  footprint: string;
  runsDir: string;
  summary: string;
  report: string;
}

export function sessionPaths(sessionDir: string): SessionPaths {
  return {
    dir: sessionDir,
    session: join(sessionDir, 'session.json'),
    environment: join(sessionDir, 'environment.json'),
    supportMatrix: join(sessionDir, 'support-matrix.json'),
    footprint: join(sessionDir, 'footprint.json'),
    runsDir: join(sessionDir, 'runs'),
    summary: join(sessionDir, 'summary.json'),
    report: join(sessionDir, 'report.md'),
  };
}

export function runRecordPath(sessionDir: string, runId: string): string {
  return join(sessionPaths(sessionDir).runsDir, `${runId}.json`);
}

/** Creates `results/raw/<sessionId>/runs/` and returns the session's paths. */
export async function createSessionDir(sessionId: string): Promise<SessionPaths> {
  const paths = sessionPaths(join(rawResultsDir, sessionId));
  await mkdir(paths.runsDir, { recursive: true });
  return paths;
}

/** Writes pretty JSON atomically (temp file plus rename), so a crash never leaves half a file. */
export async function writeJson(path: string, value: unknown): Promise<void> {
  const temporaryPath = `${path}.${process.pid}.tmp`;
  await writeFile(temporaryPath, `${JSON.stringify(value, null, 2)}\n`);
  await rename(temporaryPath, path);
}

export async function readJson(path: string): Promise<unknown> {
  const text = await readFile(path, 'utf8');
  try {
    return JSON.parse(text) as unknown;
  } catch (error) {
    throw new Error(`${path} is not valid JSON`, { cause: error });
  }
}

async function isFile(path: string): Promise<boolean> {
  try {
    return (await stat(path)).isFile();
  } catch {
    return false;
  }
}

/** The newest session directory that has a `session.json`, or null when there is none. */
export async function latestSessionDir(): Promise<string | null> {
  let names: string[];
  try {
    names = await readdir(rawResultsDir);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      return null;
    }
    throw error;
  }
  const sessionNames = names
    .filter((name) => sessionIdPattern.test(name))
    .sort()
    .reverse();
  for (const name of sessionNames) {
    const dir = join(rawResultsDir, name);
    if (await isFile(sessionPaths(dir).session)) {
      return dir;
    }
  }
  return null;
}
