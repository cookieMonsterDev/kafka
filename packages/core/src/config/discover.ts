import { existsSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { defaultOnConfigDiagnostic, type OnConfigDiagnostic } from './diagnostics';

/**
 * Extension search order for a `kafka.config.*` / `.config/kafka.*` candidate. `.ts` first: this
 * repo family is TypeScript-first and Node runs `.ts` natively, so a stray `kafka.config.js`
 * beside a `.ts` file is almost always a stale build artifact.
 */
const CANDIDATE_EXTENSIONS = ['.ts', '.mts', '.cts', '.js', '.mjs', '.cjs', '.json'] as const;

function topLevelCandidates(dir: string): string[] {
  return CANDIDATE_EXTENSIONS.map((ext) => join(dir, `kafka.config${ext}`));
}

function nestedCandidates(dir: string): string[] {
  return CANDIDATE_EXTENSIONS.map((ext) => join(dir, '.config', `kafka${ext}`));
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

function resolveInDirectory(dir: string, onDiagnostic: OnConfigDiagnostic): string | null {
  const found = topLevelCandidates(dir).filter((candidate) => existsSync(candidate));
  if (found.length > 0) {
    return reportAndPickFirst(found, dir, onDiagnostic);
  }

  const foundNested = nestedCandidates(dir).filter((candidate) => existsSync(candidate));
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
  /** Walk upward toward the filesystem root, stopping at a workspace boundary. Default `true`. */
  searchParents?: boolean;
  onDiagnostic?: OnConfigDiagnostic;
}

/**
 * Finds a `kafka.config.*` / `.config/kafka.*` file starting at `cwd`. The first directory
 * containing any candidate wins entirely — configs at multiple levels are never merged. Search
 * stops (inclusive of the boundary directory itself) at the first `.git`, `pnpm-workspace.yaml`,
 * or workspace `package.json` it finds, or immediately when `searchParents` is `false`.
 */
export function discoverConfigFile({
  cwd,
  searchParents = true,
  onDiagnostic = defaultOnConfigDiagnostic,
}: DiscoverConfigFileOptions): string | null {
  let dir = cwd;

  for (;;) {
    const found = resolveInDirectory(dir, onDiagnostic);
    if (found != null) return found;

    if (!searchParents || isSearchBoundary(dir)) return null;

    const parent = dirname(dir);
    if (parent === dir) return null;
    dir = parent;
  }
}
