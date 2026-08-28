import { existsSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { defaultOnConfigDiagnostic, type OnConfigDiagnostic } from './diagnostics';

/**
 * Extension search order for a `kafka.config.*` / `.config/kafka.*` candidate. `.ts` first: this
 * repo family is TypeScript-first and Node runs `.ts` natively, so a stray `kafka.config.js`
 * beside a `.ts` file is almost always a stale build artifact.
 */
export const CANDIDATE_EXTENSIONS = ['.ts', '.mts', '.cts', '.js', '.mjs', '.cjs', '.json'] as const;

function topLevelCandidates(dir: string, name: string): string[] {
  return CANDIDATE_EXTENSIONS.map((ext) => join(dir, `${name}.config${ext}`));
}

function nestedCandidates(dir: string, name: string): string[] {
  return CANDIDATE_EXTENSIONS.map((ext) => join(dir, '.config', `${name}${ext}`));
}

/** `.git`, `pnpm-workspace.yaml`, or a `package.json` carrying a `workspaces` field. */
function isSearchBoundary(dir: string): boolean {
  if (existsSync(join(dir, '.git'))) return true;
  if (existsSync(join(dir, 'pnpm-workspace.yaml'))) return true;

  const packageJsonPath = join(dir, 'package.json');
  if (!existsSync(packageJsonPath)) return false;

  try {
    const packageJson: unknown = JSON.parse(readFileSync(packageJsonPath, 'utf8'));
    return (
      typeof packageJson === 'object' &&
      packageJson !== null &&
      'workspaces' in packageJson &&
      (packageJson as { workspaces?: unknown }).workspaces !== undefined
    );
  } catch {
    return false;
  }
}

function resolveInDirectory(dir: string, name: string, onDiagnostic: OnConfigDiagnostic): string | null {
  const found = topLevelCandidates(dir, name).filter((candidate) => existsSync(candidate));
  if (found.length > 0) {
    return reportAndPickFirst(found, dir, onDiagnostic);
  }

  const foundNested = nestedCandidates(dir, name).filter((candidate) => existsSync(candidate));
  if (foundNested.length > 0) {
    return reportAndPickFirst(foundNested, join(dir, '.config'), onDiagnostic);
  }

  return null;
}

function reportAndPickFirst(candidates: string[], location: string, onDiagnostic: OnConfigDiagnostic): string {
  const [winner, ...rest] = candidates;
  if (winner === undefined) {
    throw new Error('Invariant violated: reportAndPickFirst called with no candidates');
  }

  if (rest.length > 0) {
    onDiagnostic({
      code: 'config.multiple-candidates',
      level: 'warn',
      message: `Multiple kafka config candidates found in ${location}: ${candidates.join(', ')}. Using ${winner}`,
      path: winner,
      candidates,
    });
  }

  return winner;
}

export interface DiscoverConfigFileOptions {
  /** Directory to start the search from. */
  cwd: string;
  /**
   * Base name for the candidate ladder: `<name>.config.*` at the top level, `.config/<name>.*`
   * nested. Default `'kafka'`. A different consumer passes its own name to discover
   * `<name>.config.ts` instead.
   */
  name?: string;
  /** Walk upward toward the filesystem root, stopping at a workspace boundary. Default `true`. */
  searchParents?: boolean;
  onDiagnostic?: OnConfigDiagnostic;
}

/**
 * Finds a `<name>.config.*` / `.config/<name>.*` file starting at `cwd` (resolved against
 * `process.cwd()` first if relative, so every returned path is absolute — required downstream by
 * `pathToFileURL` and by the sync loader's per-absolute-path memoisation). The first directory
 * containing any candidate wins
 * entirely — configs at multiple levels are never merged. Search stops (inclusive of the boundary
 * directory itself) at the first `.git`, `pnpm-workspace.yaml`, or workspace `package.json` it
 * finds, or immediately when `searchParents` is `false`.
 */
export function discoverConfigFile({
  cwd,
  name = 'kafka',
  searchParents = true,
  onDiagnostic = defaultOnConfigDiagnostic,
}: DiscoverConfigFileOptions): string | null {
  let dir = resolve(cwd);

  for (;;) {
    const found = resolveInDirectory(dir, name, onDiagnostic);
    if (found != null) return found;

    if (!searchParents || isSearchBoundary(dir)) return null;

    const parent = dirname(dir);
    if (parent === dir) return null;
    dir = parent;
  }
}
