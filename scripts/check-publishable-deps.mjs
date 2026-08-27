#!/usr/bin/env node
import { readFileSync, readdirSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import semver from 'semver';

const WORKSPACE_SCOPE = '@cookiemonsterdev/';
const DEPENDENCY_FIELDS = ['dependencies', 'peerDependencies', 'optionalDependencies'];

// D1's extraction trigger: the config loader must stay dependency-free, or move to its own
// package. Bumping this list is a deliberate decision, not a drive-by dependency add.
export const CORE_REQUIRED_DEPENDENCIES = ['lz4-lite', 'snappyjs'];

export function checkManifest(manifest, workspaceVersions = {}) {
  const problems = [];
  const name = manifest.name ?? '<unnamed package>';

  if (!manifest.private) {
    for (const field of DEPENDENCY_FIELDS) {
      const deps = manifest[field];
      if (!deps) continue;

      for (const [depName, range] of Object.entries(deps)) {
        if (typeof range === 'string' && range.startsWith('workspace:')) {
          problems.push(
            `${name}: ${field}["${depName}"] is "${range}" — npm publish ships workspace: specifiers ` +
              'verbatim, producing an uninstallable tarball; use a plain semver range instead',
          );
          continue;
        }

        if (depName.startsWith(WORKSPACE_SCOPE) && depName in workspaceVersions) {
          const localVersion = workspaceVersions[depName];
          if (!semver.satisfies(localVersion, range)) {
            problems.push(
              `${name}: ${field}["${depName}"] range "${range}" does not admit the local ` +
                `version ${localVersion} — bump the range or the local package`,
            );
          }
        }
      }
    }
  }

  if (name === '@cookiemonsterdev/kafka-core') {
    const actual = Object.keys(manifest.dependencies ?? {}).sort();
    const expected = [...CORE_REQUIRED_DEPENDENCIES].sort();
    if (actual.join(',') !== expected.join(',')) {
      problems.push(
        `${name}: dependencies must be exactly {${expected.join(', ')}} (found ` +
          `{${actual.join(', ') || 'none'}}) — see the D1 extraction trigger in the config-loader plan`,
      );
    }
  }

  return problems;
}

export function checkWorkspace(manifests) {
  const workspaceVersions = Object.fromEntries(
    manifests.filter((manifest) => manifest.name).map((manifest) => [manifest.name, manifest.version]),
  );
  return manifests.flatMap((manifest) => checkManifest(manifest, workspaceVersions));
}

export function readWorkspaceManifests(root) {
  const packagesDir = path.join(root, 'packages');
  return readdirSync(packagesDir, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => path.join(packagesDir, entry.name, 'package.json'))
    .map((file) => JSON.parse(readFileSync(file, 'utf8')));
}

function main() {
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
  const manifests = readWorkspaceManifests(root);
  const problems = checkWorkspace(manifests);

  if (problems.length > 0) {
    console.error('Publishable-dependency check failed:\n');
    for (const problem of problems) console.error(`  - ${problem}`);
    process.exitCode = 1;
    return;
  }

  console.log(`Publishable-dependency check passed for ${manifests.length} package(s).`);
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) {
  main();
}
