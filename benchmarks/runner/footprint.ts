import { clientIds, clientPackageNames } from '../client/spec.ts';
import type { ClientId } from '../client/spec.ts';
import { runtimeBinary } from './images.ts';
import { docker } from './process.ts';
import type { Footprint, RuntimeImage } from './types.ts';

/**
 * Runs inside the image (Node or Bun, CommonJS-style `-e` script). For each package it walks the
 * production dependency closure (`dependencies` plus installed `optionalDependencies`, resolved with
 * Node's lookup rules from each package's real directory, so pnpm's symlinked layout and npm's flat one
 * both work) and sums the apparent size of every file. Nested `node_modules` directories are skipped
 * while sizing a package because the packages inside them are counted through the closure.
 */
const footprintScript = `
const fs = require('node:fs');
const path = require('node:path');

const packages = JSON.parse(process.argv.at(-1));
const benchRoot = '/bench';

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

function scanPackage(packageDir) {
  let bytes = 0;
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
      if (entry.name.endsWith('.node') || entry.name === 'binding.gyp') native = true;
    }
  }
  return { bytes, native };
}

function measure(name) {
  const rootDir = findPackageDir(benchRoot, name);
  if (rootDir === null) return { version: null, installBytes: null, dependencyCount: null, hasNativeAddon: null };

  const version = JSON.parse(fs.readFileSync(path.join(rootDir, 'package.json'), 'utf8')).version ?? null;
  const visited = new Set([rootDir]);
  const queue = [rootDir];
  let installBytes = 0;
  let hasNativeAddon = false;
  while (queue.length > 0) {
    const packageDir = queue.shift();
    const scanned = scanPackage(packageDir);
    installBytes += scanned.bytes;
    hasNativeAddon = hasNativeAddon || scanned.native;

    const manifest = JSON.parse(fs.readFileSync(path.join(packageDir, 'package.json'), 'utf8'));
    const dependencyNames = Object.keys({ ...manifest.dependencies, ...manifest.optionalDependencies });
    for (const dependencyName of dependencyNames) {
      const dependencyDir = findPackageDir(packageDir, dependencyName);
      if (dependencyDir === null || visited.has(dependencyDir)) continue;
      visited.add(dependencyDir);
      queue.push(dependencyDir);
    }
  }
  return { version, installBytes, dependencyCount: visited.size - 1, hasNativeAddon };
}

const footprints = Object.entries(packages).map(([client, packageName]) => ({ client, packageName, ...measure(packageName) }));
console.log(JSON.stringify(footprints));
`;

function nullableNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function parseFootprints(output: string, reference: string): Footprint[] {
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
    return {
      client,
      packageName: clientPackageNames[client],
      version: typeof entry.version === 'string' ? entry.version : null,
      installBytes: nullableNumber(entry.installBytes),
      dependencyCount: nullableNumber(entry.dependencyCount),
      hasNativeAddon: typeof entry.hasNativeAddon === 'boolean' ? entry.hasNativeAddon : null,
    };
  });
}

/**
 * Static F1 data for every client, measured inside `image` with one short `docker run`: install size of
 * the package and its production dependency closure, closure size, native addon (any `.node` file or
 * `binding.gyp` in the closure), and version. Fields are null when the package is not installed.
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
    JSON.stringify(clientPackageNames),
  ]);
  return parseFootprints(output, image.reference);
}
