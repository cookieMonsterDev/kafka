import { cp, mkdir, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { clientIds, clientPackageNames } from '../client/spec.ts';
import type { ClientId, RuntimeId } from '../client/spec.ts';
import { exportCoreSourceTree } from './core-source.ts';
import { docker, runCommand, tailLines } from './process.ts';
import type { CommandResult } from './process.ts';
import type { CoreSource, RuntimeImage } from './types.ts';

export const imageRepository = 'kafka-bench-client';

type RuntimeBinary = 'node' | 'bun';

interface RuntimeDefinition {
  /** Base tag of the runtime stage; resolved to a digest at build time. */
  baseImage: string;
  /** Node image whose ABI the `deps` stage builds native addons for. */
  depsImage: string;
  target: 'runtime-node' | 'runtime-bun';
  binary: RuntimeBinary;
}

// All three runtimes use Debian 13 (trixie) so every cell has the same glibc and allocator. oven/bun
// publishes no Debian 12 variant, and `1-debian` is the newest Debian-based tag on the 1.x line.
const node24Image = 'node:24-trixie-slim';
const node26Image = 'node:26-trixie-slim';
const bunImage = 'oven/bun:1-debian';
/** kafka-core and kafka-config are built with the repository's own Node version (`.nvmrc`). */
const coreBuilderImage = node26Image;

export const runtimeDefinitions: Readonly<Record<RuntimeId, RuntimeDefinition>> = {
  node24: { baseImage: node24Image, depsImage: node24Image, target: 'runtime-node', binary: 'node' },
  node26: { baseImage: node26Image, depsImage: node26Image, target: 'runtime-node', binary: 'node' },
  // Bun reuses a Node `deps` stage. Bun 1.4 reports Node 26 (`process.versions.modules` 147) and refuses
  // addons built for another ABI ("compiled against ... NODE_MODULE_VERSION 137"), so it takes the Node 26 tree.
  // Revisit when a Bun release reports a different Node version.
  bun: { baseImage: bunImage, depsImage: node26Image, target: 'runtime-bun', binary: 'bun' },
};

export function runtimeBinary(runtime: RuntimeId): RuntimeBinary {
  return runtimeDefinitions[runtime].binary;
}

interface ResolvedBaseImage {
  tag: string;
  /** `sha256:...` of the multi-platform index (or manifest) the tag pointed to. */
  digest: string;
  /** `tag@digest`, used as the build argument so the build cannot drift from the recorded digest. */
  pinnedReference: string;
}

function repositoryOf(tag: string): string {
  const lastSlash = tag.lastIndexOf('/');
  const lastColon = tag.lastIndexOf(':');
  return lastColon > lastSlash ? tag.slice(0, lastColon) : tag;
}

/** `docker image inspect` arguments whose output `parseRepoDigest` reads. */
export function repoDigestsInspectArgs(tag: string): string[] {
  return ['image', 'inspect', '--format', '{{json .RepoDigests}}', tag];
}

/**
 * The registry digest (`sha256:...`) that `tag`'s repository has in the JSON output of
 * `repoDigestsInspectArgs`, or null when the image has none (built locally, never pulled).
 */
export function parseRepoDigest(tag: string, repoDigestsJson: string): string | null {
  const repoDigests: unknown = JSON.parse(repoDigestsJson);
  if (repoDigests === null) return null;
  if (!Array.isArray(repoDigests)) throw new Error(`unexpected RepoDigests for ${tag}: ${repoDigestsJson.trim()}`);

  const repository = repositoryOf(tag);
  const shortRepository = repository.replace(/^docker\.io\//, '').replace(/^library\//, '');
  const match = repoDigests
    .filter((entry): entry is string => typeof entry === 'string')
    .map((entry) => entry.split('@'))
    .find(([name, digest]) => (name === repository || name === shortRepository) && digest?.startsWith('sha256:'));
  return match?.[1] ?? null;
}

async function resolveBaseImage(tag: string, log: (line: string) => void): Promise<ResolvedBaseImage> {
  const pull = await runCommand('docker', ['pull', '--quiet', tag], { allowFailure: true });
  if (pull.exitCode !== 0) log(`could not pull ${tag}, using the local copy: ${tailLines(pull.stderr, 1)}`);

  const output = await docker(repoDigestsInspectArgs(tag));
  const digest = parseRepoDigest(tag, output);
  if (digest === null) throw new Error(`no registry digest for ${tag} (RepoDigests: ${output.trim()})`);

  return { tag, digest, pinnedReference: `${tag}@${digest}` };
}

async function resolveBaseImages(
  tags: readonly string[],
  log: (line: string) => void,
): Promise<Map<string, ResolvedBaseImage>> {
  const resolved = new Map<string, ResolvedBaseImage>();
  for (const tag of new Set(tags)) {
    const image = await resolveBaseImage(tag, log);
    log(`base image ${image.pinnedReference}`);
    resolved.set(tag, image);
  }
  return resolved;
}

function requireResolved(resolved: Map<string, ResolvedBaseImage>, tag: string): ResolvedBaseImage {
  const image = resolved.get(tag);
  if (image === undefined) throw new Error(`base image ${tag} was not resolved`);
  return image;
}

/** Files of the benchmark project that go into the build context; everything else (node_modules, results) stays out. */
const benchmarkContextEntries = [
  'package.json',
  'pnpm-lock.yaml',
  'pnpm-workspace.yaml',
  'tsconfig.json',
  'client',
  'runner',
];

interface BuildContext {
  directory: string;
  dockerfile: string;
}

/**
 * Assembles a throwaway build context:
 *
 *   core-src/  the repository at the requested commit (or the worktree), without `benchmarks/`
 *   bench/     the benchmark project files listed in `benchmarkContextEntries`
 */
async function createBuildContext(core: CoreSource, repoRoot: string): Promise<BuildContext> {
  const directory = await mkdtemp(join(tmpdir(), 'kafka-bench-context-'));
  try {
    await exportCoreSourceTree(core, repoRoot, join(directory, 'core-src'));
    const benchmarksDir = join(repoRoot, 'benchmarks');
    const benchDir = join(directory, 'bench');
    await mkdir(benchDir);
    for (const entry of benchmarkContextEntries) {
      await cp(join(benchmarksDir, entry), join(benchDir, entry), {
        recursive: true,
        filter: (source) => !source.split('/').includes('node_modules'),
      });
    }
    return { directory, dockerfile: join(benchmarksDir, 'docker', 'Dockerfile') };
  } catch (error) {
    await rm(directory, { recursive: true, force: true });
    throw error;
  }
}

/** Build output is long (pnpm, node-gyp); only step headers and errors go to the log. */
function isNotableBuildLine(line: string): boolean {
  return /^#\d+ \[/.test(line) || /\berror\b/i.test(line);
}

interface BuildInvocation {
  context: BuildContext;
  target: string;
  buildArgs: Record<string, string>;
  tags?: readonly string[];
  labels?: Record<string, string>;
  /** `cacheonly` builds the target without exporting an image. */
  output?: 'image' | 'cacheonly';
  log: (line: string) => void;
}

async function dockerBuild(invocation: BuildInvocation): Promise<CommandResult> {
  const args = ['build', '--progress=plain', '--file', invocation.context.dockerfile, '--target', invocation.target];
  for (const [name, value] of Object.entries(invocation.buildArgs)) args.push('--build-arg', `${name}=${value}`);
  for (const [name, value] of Object.entries(invocation.labels ?? {})) args.push('--label', `${name}=${value}`);
  for (const tag of invocation.tags ?? []) args.push('--tag', tag);
  if (invocation.output === 'cacheonly') args.push('--output', 'type=cacheonly');
  args.push(invocation.context.directory);

  return runCommand('docker', args, {
    allowFailure: true,
    onLine: (line) => {
      if (isNotableBuildLine(line)) invocation.log(line);
    },
  });
}

function coreBuildArgs(core: CoreSource): Record<string, string> {
  return { CORE_SHA: core.sha };
}

function imageTagSuffix(core: CoreSource): string {
  const shortSha = core.sha.slice(0, 12);
  return core.isDirty ? `${shortSha}-dirty` : shortSha;
}

interface ImageRuntimeInfo {
  runtimeVersion: string;
  clientVersions: Record<ClientId, string | null>;
}

/** Runs inside the image under its own runtime; prints the runtime version and every installed client version as JSON. */
const runtimeInfoScript = `
const { readFileSync } = require('node:fs');
const packages = JSON.parse(process.argv.at(-1));
const clientVersions = {};
for (const [client, name] of Object.entries(packages)) {
  try {
    clientVersions[client] = JSON.parse(readFileSync('/bench/node_modules/' + name + '/package.json', 'utf8')).version ?? null;
  } catch {
    clientVersions[client] = null;
  }
}
const runtimeVersion = process.versions.bun ?? process.versions.node;
console.log(JSON.stringify({ runtimeVersion, clientVersions }));
`;

function parseRuntimeInfo(output: string, reference: string): ImageRuntimeInfo {
  const lastLine = output.trim().split('\n').at(-1) ?? '';
  let parsed: unknown;
  try {
    parsed = JSON.parse(lastLine);
  } catch (error) {
    throw new Error(`runtime info from ${reference} is not JSON: ${lastLine}`, { cause: error });
  }
  if (typeof parsed !== 'object' || parsed === null) throw new Error(`runtime info from ${reference} is not an object`);
  const { runtimeVersion, clientVersions } = parsed as { runtimeVersion?: unknown; clientVersions?: unknown };
  if (typeof runtimeVersion !== 'string' || runtimeVersion.length === 0) {
    throw new Error(`runtime info from ${reference} has no runtime version: ${lastLine}`);
  }
  if (typeof clientVersions !== 'object' || clientVersions === null) {
    throw new Error(`runtime info from ${reference} has no client versions: ${lastLine}`);
  }

  const versions = {} as Record<ClientId, string | null>;
  for (const client of clientIds) {
    const version = (clientVersions as Record<string, unknown>)[client];
    versions[client] = typeof version === 'string' ? version : null;
  }
  return { runtimeVersion, clientVersions: versions };
}

async function inspectRuntime(reference: string, runtime: RuntimeId): Promise<ImageRuntimeInfo> {
  // `bun -e` and `node -e` both accept CommonJS-style scripts with `require`.
  const output = await docker([
    'run',
    '--rm',
    '--network',
    'none',
    '--entrypoint',
    runtimeBinary(runtime),
    reference,
    '-e',
    runtimeInfoScript,
    JSON.stringify(clientPackageNames),
  ]);
  return parseRuntimeInfo(output, reference);
}

export interface BuildImagesOptions {
  core: CoreSource;
  runtimes: readonly RuntimeId[];
  repoRoot: string;
  log: (line: string) => void;
}

/**
 * Builds one client image per runtime from `docker/Dockerfile`, tagged
 * `kafka-bench-client:<runtime>-<sha12>` (suffix `-dirty` for a dirty worktree) and `kafka-bench-client:<runtime>`.
 * Base tags are pulled and pinned to their current digest first, so the recorded digest is the one built from.
 * The kafka-core build and the dependency installs are shared between runtimes through the build cache.
 */
export async function buildImages(options: BuildImagesOptions): Promise<RuntimeImage[]> {
  const { core, runtimes, repoRoot, log } = options;
  if (runtimes.length === 0) return [];

  const definitions = runtimes.map((runtime) => ({ runtime, definition: runtimeDefinitions[runtime] }));
  const baseImages = await resolveBaseImages(
    [coreBuilderImage, ...definitions.flatMap(({ definition }) => [definition.depsImage, definition.baseImage])],
    log,
  );

  const context = await createBuildContext(core, repoRoot);
  try {
    const images: RuntimeImage[] = [];
    for (const { runtime, definition } of definitions) {
      const base = requireResolved(baseImages, definition.baseImage);
      const reference = `${imageRepository}:${runtime}-${imageTagSuffix(core)}`;
      log(`building ${reference} (${definition.target} on ${base.pinnedReference})`);
      const startedAtMs = performance.now();

      const result = await dockerBuild({
        context,
        target: definition.target,
        buildArgs: {
          ...coreBuildArgs(core),
          CORE_BUILDER_IMAGE: requireResolved(baseImages, coreBuilderImage).pinnedReference,
          DEPS_IMAGE: requireResolved(baseImages, definition.depsImage).pinnedReference,
          RUNTIME_IMAGE: base.pinnedReference,
        },
        tags: [reference, `${imageRepository}:${runtime}`],
        labels: {
          'org.opencontainers.image.revision': core.sha,
          'kafka-bench.core-ref': core.requestedRef,
          'kafka-bench.core-dirty': String(core.isDirty),
          'kafka-bench.runtime': runtime,
          'kafka-bench.base-image': base.tag,
          'kafka-bench.base-digest': base.digest,
        },
        log,
      });
      if (result.exitCode !== 0) {
        throw new Error(
          `building ${reference} failed (exit ${String(result.exitCode)}):\n${tailLines(result.stderr, 60)}`,
        );
      }

      const imageId = (await docker(['image', 'inspect', '--format', '{{.Id}}', reference])).trim();
      const info = await inspectRuntime(reference, runtime);
      const elapsedSeconds = ((performance.now() - startedAtMs) / 1000).toFixed(1);
      log(`built ${reference} in ${elapsedSeconds}s: ${runtime} ${info.runtimeVersion}, image ${imageId}`);

      images.push({
        runtime,
        reference,
        imageId,
        baseImage: base.tag,
        baseDigest: base.digest,
        runtimeVersion: info.runtimeVersion,
        clientVersions: info.clientVersions,
      });
    }
    return images;
  } finally {
    await rm(context.directory, { recursive: true, force: true });
  }
}

export interface TypecheckOptions {
  core: CoreSource;
  repoRoot: string;
  log: (line: string) => void;
}

export interface TypecheckResult {
  passed: boolean;
  /** The compiler output (or the build failure) when it did not pass; empty otherwise. */
  output: string;
}

/**
 * Type-checks `client/` and `runner/` with `tsc --noEmit` inside the `typecheck` stage of the bench image,
 * against the clients installed there (Node 24 dependency tree, all devDependencies). Equivalent one-liner,
 * given a context assembled as in `createBuildContext`:
 * `docker build --target typecheck --output type=cacheonly -f benchmarks/docker/Dockerfile <context>`.
 */
export async function typecheckInImage(options: TypecheckOptions): Promise<TypecheckResult> {
  const { core, repoRoot, log } = options;
  const baseImages = await resolveBaseImages([coreBuilderImage, node24Image], log);
  const context = await createBuildContext(core, repoRoot);
  try {
    const result = await dockerBuild({
      context,
      target: 'typecheck',
      buildArgs: {
        ...coreBuildArgs(core),
        CORE_BUILDER_IMAGE: requireResolved(baseImages, coreBuilderImage).pinnedReference,
        DEPS_IMAGE: requireResolved(baseImages, node24Image).pinnedReference,
      },
      output: 'cacheonly',
      log,
    });
    if (result.exitCode === 0) return { passed: true, output: '' };
    return { passed: false, output: extractTypecheckOutput(result.stderr) };
  } finally {
    await rm(context.directory, { recursive: true, force: true });
  }
}

/** Pulls the compiler's lines out of BuildKit's plain progress output (`#12 1.234 client/main.ts(3,1): error ...`). */
function extractTypecheckOutput(buildLog: string): string {
  const compilerLines = buildLog
    .split('\n')
    .map((line) => /^#\d+ \d+\.\d+ (.*)$/.exec(line)?.[1])
    .filter((line): line is string => line !== undefined && /error TS\d+|\.ts\(\d+,\d+\)/.test(line));
  if (compilerLines.length > 0) return compilerLines.join('\n');
  return tailLines(buildLog, 60);
}
