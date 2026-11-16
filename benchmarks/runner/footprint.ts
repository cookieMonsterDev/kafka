import { clientIds, clientPackageNames } from '../client/spec.ts';
import type { ClientId } from '../client/spec.ts';
import { runtimeBinary } from './images.ts';
import { docker } from './process.ts';
import type { Footprint, FootprintPart, RuntimeImage } from './types.ts';

/** Directory inside the image that holds the installed clients (see `docker/Dockerfile`). */
const imageInstallRoot = '/bench';

/**
 * Runs on Node or Bun as a CommonJS-style `-e` script, with the install root and the JSON map of client id
 * to package name as its last two arguments. For each package it walks the dependency closure, resolving
 * names with Node's lookup rules from each package's real directory, so pnpm's symlinked layout and npm's
 * flat one both work. The required part is the package and everything it reaches through `dependencies`
 * alone; the optional part is what installed `optionalDependencies` add on top. Each part sums the apparent
 * size and number of its files. Nested `node_modules` directories are skipped while sizing a package
 * because the packages inside them are counted through the closure.
 */
export const footprintScript = `
const fs = require('node:fs');
const path = require('node:path');

const [installRoot, packagesJson] = process.argv.slice(-2);
const packages = JSON.parse(packagesJson);

function ancestors(start) {
  const directories = [start];
  while (path.dirname(directories.at(-1)) !== directories.at(-1)) directories.push(path.dirname(directories.at(-1)));
  return directories;
}

function findPackageDir(fromDir, name) {
  for (const directory of ancestors(fromDir)) {
    const candidate = path.join(directory, 'node_modules', name);
    if (fs.existsSync(path.join(candidate, 'package.json'))) return fs.realpathSync(candidate);
  }
  return null;
}

function readManifest(packageDir) {
  return JSON.parse(fs.readFileSync(path.join(packageDir, 'package.json'), 'utf8'));
}

function dependencyNames(manifest, { includeOptional }) {
  const optionalNames = Object.keys(manifest.optionalDependencies ?? {});
  const names = Object.keys(manifest.dependencies ?? {});
  if (includeOptional) return [...names, ...optionalNames];
  // As in npm, an optional dependency overrides a regular one of the same name.
  return names.filter((name) => !optionalNames.includes(name));
}

/** Real directories of the packages reachable from rootDir, rootDir first. */
function dependencyClosure(rootDir, options) {
  const visited = new Set([rootDir]);
  const queue = [rootDir];
  while (queue.length > 0) {
    const packageDir = queue.shift();
    for (const dependencyName of dependencyNames(readManifest(packageDir), options)) {
      const dependencyDir = findPackageDir(packageDir, dependencyName);
      if (dependencyDir === null || visited.has(dependencyDir)) continue;
      visited.add(dependencyDir);
      queue.push(dependencyDir);
    }
  }
  return visited;
}

function scanPackage(packageDir) {
  let bytes = 0;
  let fileCount = 0;
  let native = false;
  const pending = [packageDir];
  while (pending.length > 0) {
    const directory = pending.pop();
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const entryPath = path.join(directory, entry.name);
      if (entry.isDirectory()) {
        if (entry.name !== 'node_modules') pending.push(entryPath);
        continue;
      }
      if (!entry.isFile()) continue;
      bytes += fs.statSync(entryPath).size;
      fileCount += 1;
      if (entry.name.endsWith('.node') || entry.name === 'binding.gyp') native = true;
    }
  }
  return { bytes, fileCount, native };
}

function measurePart(packageDirs, dependencyCount) {
  const part = { bytes: 0, fileCount: 0, dependencyCount, hasNativeAddon: false };
  for (const packageDir of packageDirs) {
    const scanned = scanPackage(packageDir);
    part.bytes += scanned.bytes;
    part.fileCount += scanned.fileCount;
    part.hasNativeAddon = part.hasNativeAddon || scanned.native;
  }
  return part;
}

function measure(name) {
  const rootDir = findPackageDir(installRoot, name);
  if (rootDir === null) return { version: null, required: null, optional: null };

  const required = dependencyClosure(rootDir, { includeOptional: false });
  const optional = [...dependencyClosure(rootDir, { includeOptional: true })].filter(
    (packageDir) => !required.has(packageDir),
  );
  return {
    version: readManifest(rootDir).version ?? null,
    required: measurePart(required, required.size - 1),
    optional: measurePart(optional, optional.length),
  };
}

const footprints = Object.entries(packages).map(([client, packageName]) => ({ client, packageName, ...measure(packageName) }));
console.log(JSON.stringify(footprints));
`;

function isCount(value: unknown): value is number {
  return typeof value === 'number' && Number.isInteger(value) && value >= 0;
}

function parsePart(value: unknown): FootprintPart | null {
  if (typeof value !== 'object' || value === null) return null;
  const { bytes, fileCount, dependencyCount, hasNativeAddon } = value as Record<string, unknown>;
  if (!isCount(bytes) || !isCount(fileCount) || !isCount(dependencyCount) || typeof hasNativeAddon !== 'boolean') {
    return null;
  }
  return { bytes, fileCount, dependencyCount, hasNativeAddon };
}

/** Parses the script's last output line into one footprint per client id, in `clientIds` order. */
export function parseFootprints(output: string, reference: string): Footprint[] {
  const lastLine = output.trim().split('\n').at(-1) ?? '';
  let parsed: unknown;
  try {
    parsed = JSON.parse(lastLine);
  } catch (error) {
    throw new Error(`footprint output from ${reference} is not JSON: ${lastLine}`, { cause: error });
  }
  if (!Array.isArray(parsed)) throw new Error(`footprint output from ${reference} is not an array: ${lastLine}`);

  const byClient = new Map<string, Record<string, unknown>>();
  for (const entry of parsed) {
    if (typeof entry === 'object' && entry !== null && typeof (entry as { client?: unknown }).client === 'string') {
      byClient.set((entry as { client: string }).client, entry as Record<string, unknown>);
    }
  }

  return clientIds.map((client: ClientId): Footprint => {
    const entry = byClient.get(client);
    if (entry === undefined) throw new Error(`footprint output from ${reference} has no entry for ${client}`);
    const required = parsePart(entry.required);
    const optional = parsePart(entry.optional);
    // A package that is installed always has both parts, even when the optional one is empty.
    const isInstalled = required !== null && optional !== null;
    return {
      client,
      packageName: clientPackageNames[client],
      version: typeof entry.version === 'string' ? entry.version : null,
      required: isInstalled ? required : null,
      optional: isInstalled ? optional : null,
    };
  });
}

/**
 * Static F1 data for every client, measured inside `image` with one short `docker run`: the version, and
 * for the required and the optional part of the package's dependency closure, the installed bytes, files
 * and dependencies, and whether it holds a native addon (any `.node` file or `binding.gyp`). Both parts
 * are null when the package is not installed.
 */
export async function measureFootprints(image: RuntimeImage): Promise<Footprint[]> {
  const output = await docker([
    'run',
    '--rm',
    '--network',
    'none',
    '--entrypoint',
    runtimeBinary(image.runtime),
    image.reference,
    '-e',
    footprintScript,
    imageInstallRoot,
    JSON.stringify(clientPackageNames),
  ]);
  return parseFootprints(output, image.reference);
}
