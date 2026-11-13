import { cp, lstat, mkdir, readlink, rm, symlink } from 'node:fs/promises';
import { dirname, join } from 'node:path';

import { runCommand } from './process.ts';
import type { CommandResult } from './process.ts';
import type { CoreSource } from './types.ts';

const developRemote = 'origin';
const developBranch = 'develop';

/** The `--core-ref` default, and the branch a published session's kafka-core commit must be on. */
export const defaultCoreRef = `${developRemote}/${developBranch}`;
/** `--core-ref` value that builds kafka-core from the current checkout instead of a commit. */
export const worktreeRef = 'WORKTREE';

/** The benchmark project never goes into the kafka-core build context or the dirty check. */
const benchmarksExcludePathspec = ':(exclude)benchmarks';

// `ls-files` over a large checkout can be long; output that reaches this limit fails instead of being truncated.
const gitMaxCapturedChars = 256 * 1024 * 1024;

/** Runs git and resolves with its exit code and output; throws only when git cannot run or is killed. */
async function runGit(repoRoot: string, args: readonly string[]): Promise<CommandResult & { exitCode: number }> {
  const result = await runCommand('git', args, {
    cwd: repoRoot,
    allowFailure: true,
    maxCapturedChars: gitMaxCapturedChars,
  });
  if (result.exitCode === null) throw new Error(`git ${args.join(' ')} was killed by a signal`);
  if (result.stdout.length >= gitMaxCapturedChars) {
    throw new Error(`git ${args.join(' ')} printed more than ${gitMaxCapturedChars} characters`);
  }
  return { ...result, exitCode: result.exitCode };
}

async function git(repoRoot: string, args: readonly string[]): Promise<string> {
  const result = await runGit(repoRoot, args);
  if (result.exitCode !== 0) {
    const details = result.stderr.trim() || `exit code ${result.exitCode}`;
    throw new Error(`git ${args.join(' ')} failed: ${details}`);
  }
  return result.stdout;
}

function validateRef(requestedRef: string): void {
  if (requestedRef.length === 0) throw new Error('core ref is empty');
  if (requestedRef.startsWith('-')) throw new Error(`core ref must not start with "-": ${requestedRef}`);
  if (/[\s\0]/.test(requestedRef))
    throw new Error(`core ref must not contain whitespace: ${JSON.stringify(requestedRef)}`);
}

/** `origin/<branch>` refs are fetched before resolving, so they reflect the remote rather than a stale copy. */
function remoteBranchOf(requestedRef: string): string | null {
  const prefix = `${developRemote}/`;
  if (!requestedRef.startsWith(prefix)) return null;
  return requestedRef.slice(prefix.length);
}

async function fetchRemoteBranch(repoRoot: string, branch: string): Promise<void> {
  // A plain fetch only updates refs/remotes/*; the working tree and the current branch stay untouched.
  // `--` keeps a branch name that starts with `-` from being read as an option.
  await git(repoRoot, ['fetch', '--quiet', developRemote, '--', branch]);
}

async function resolveCommit(repoRoot: string, ref: string): Promise<string> {
  const output = await git(repoRoot, ['rev-parse', '--verify', '--end-of-options', `${ref}^{commit}`]);
  const sha = output.trim();
  if (!/^[0-9a-f]{40}$/.test(sha)) throw new Error(`git rev-parse returned an unexpected value for ${ref}: ${sha}`);
  return sha;
}

/** Checks against the local `origin/develop`; `isAncestorOfOriginDevelop` fetches it first. */
async function isAncestorOfDevelop(repoRoot: string, sha: string): Promise<boolean> {
  const args = ['merge-base', '--is-ancestor', sha, defaultCoreRef];
  const { exitCode, stderr } = await runGit(repoRoot, args);
  if (exitCode === 0) return true;
  if (exitCode === 1) return false;
  throw new Error(`git ${args.join(' ')} exited with ${exitCode}: ${stderr.trim()}`);
}

/**
 * Fetches `origin/develop`, then tells whether `sha` is on it. Throws when the fetch fails or git
 * reports an error (such as an unknown commit), so a failure never reads as "not on develop".
 */
export async function isAncestorOfOriginDevelop(repoRoot: string, sha: string): Promise<boolean> {
  if (!/^[0-9a-f]{40}$/.test(sha)) throw new Error(`expected a full commit SHA, got ${JSON.stringify(sha)}`);
  await fetchRemoteBranch(repoRoot, developBranch);
  return isAncestorOfDevelop(repoRoot, sha);
}

async function hasWorktreeChanges(repoRoot: string): Promise<boolean> {
  const status = await git(repoRoot, ['status', '--porcelain', '--', '.', benchmarksExcludePathspec]);
  return status.trim().length > 0;
}

/**
 * Resolves the kafka-core revision to benchmark.
 *
 * `origin/<branch>` refs (the default `origin/develop` included) are fetched first. Any other ref
 * (a SHA, tag, or local branch) is resolved as is, after a best-effort fetch of `origin/develop` so
 * `isOnOriginDevelop` is current. `WORKTREE` resolves to `HEAD`, with `isDirty` set when tracked or
 * untracked files outside `benchmarks/` differ from it.
 */
export async function resolveCoreSource(requestedRef: string, repoRoot: string): Promise<CoreSource> {
  validateRef(requestedRef);
  const remoteBranch = remoteBranchOf(requestedRef);

  if (remoteBranch !== null) {
    await fetchRemoteBranch(repoRoot, remoteBranch);
    if (remoteBranch !== developBranch) await fetchRemoteBranch(repoRoot, developBranch);
  } else {
    // Offline work on a local ref or the worktree should not fail only because the remote is unreachable.
    await fetchRemoteBranch(repoRoot, developBranch).catch(() => undefined);
  }

  const isWorktree = requestedRef === worktreeRef;
  const sha = await resolveCommit(repoRoot, isWorktree ? 'HEAD' : requestedRef);
  const commitDate = (await git(repoRoot, ['show', '-s', '--format=%cI', sha])).trim();
  const isDirty = isWorktree ? await hasWorktreeChanges(repoRoot) : false;
  const isOnOriginDevelop = await isAncestorOfDevelop(repoRoot, sha);

  return { requestedRef, sha, commitDate, isWorktree, isDirty, isOnOriginDevelop };
}

async function exportCommit(repoRoot: string, sha: string, destinationDir: string): Promise<void> {
  const archivePath = join(destinationDir, '..', `core-src-${sha}.tar`);
  try {
    await git(repoRoot, ['archive', '--format=tar', '-o', archivePath, sha, '--', '.', benchmarksExcludePathspec]);
    await runCommand('tar', ['-xf', archivePath, '-C', destinationDir]);
  } finally {
    await rm(archivePath, { force: true });
  }
}

/**
 * Copies what `git add --all && git archive` would capture: tracked files as they are on disk now,
 * plus untracked files that are not ignored. Deleted tracked files are skipped, so the build sees the
 * working tree exactly, without ignored artifacts such as `node_modules/` or `dist/`.
 */
async function exportWorktree(repoRoot: string, destinationDir: string): Promise<void> {
  const listing = await git(repoRoot, [
    'ls-files',
    '-z',
    '--cached',
    '--others',
    '--exclude-standard',
    '--',
    '.',
    benchmarksExcludePathspec,
  ]);
  const relativePaths = [...new Set(listing.split('\0').filter((path) => path.length > 0))];

  for (const relativePath of relativePaths) {
    const sourcePath = join(repoRoot, relativePath);
    const targetPath = join(destinationDir, relativePath);
    const stats = await lstat(sourcePath).catch(() => null);
    if (stats === null) continue;

    await mkdir(dirname(targetPath), { recursive: true });
    if (stats.isSymbolicLink()) {
      await symlink(await readlink(sourcePath), targetPath);
      continue;
    }
    if (!stats.isFile()) continue;
    await cp(sourcePath, targetPath, { preserveTimestamps: false });
  }
}

/**
 * Writes the repository tree for `core` into `destinationDir` (created if missing), without
 * `benchmarks/`. A commit is exported with `git archive`; `WORKTREE` copies the checkout as described
 * on `exportWorktree`.
 */
export async function exportCoreSourceTree(core: CoreSource, repoRoot: string, destinationDir: string): Promise<void> {
  await mkdir(destinationDir, { recursive: true });
  if (core.isWorktree) {
    await exportWorktree(repoRoot, destinationDir);
    return;
  }
  await exportCommit(repoRoot, core.sha, destinationDir);
}
