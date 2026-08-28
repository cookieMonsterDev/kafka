#!/usr/bin/env node
import { readFileSync, readdirSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import semver from 'semver';

const WORKSPACE_SCOPE = '@cookiemonsterdev/';
const DEPENDENCY_FIELDS = ['dependencies', 'peerDependencies', 'optionalDependencies'];

const CORE_NAME = '@cookiemonsterdev/kafka-core';
const CONFIG_NAME = '@cookiemonsterdev/kafka-config';

// core depends on kafka-config to resolve `kafka.config.*` files, plus its two third-party codecs;
// kafka-config itself stays dependency-free — a runtime dependency in either must be a deliberate
// decision, not a drive-by `pnpm add`.
function expectedDependencies() {
  return {
    [CORE_NAME]: ['lz4-lite', 'snappyjs', CONFIG_NAME],
    [CONFIG_NAME]: [],
  };
}

function checkManifest(manifest, workspaceVersions, expectedDeps) {
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

  const expected = expectedDeps[name];
  if (expected) {
    const actual = Object.keys(manifest.dependencies ?? {});
    const isWorkspace = (dep) => dep.startsWith(WORKSPACE_SCOPE);

    // Reported separately so the failure message says which rule broke: a stray third-party
    // dependency is a policy violation (this package's dependency set is meant to stay fixed), a
    // wrong workspace-internal dependency is very likely just a missed `pnpm add`.
    for (const [label, keep] of [
      ['third-party', (dep) => !isWorkspace(dep)],
      ['workspace-internal', isWorkspace],
    ]) {
      const expectedSet = new Set(expected.filter(keep).sort());
      const actualSet = new Set(actual.filter(keep).sort());
      if ([...expectedSet].join(',') !== [...actualSet].join(',')) {
        problems.push(
          `${name}: ${label} dependencies must be exactly {${[...expectedSet].join(', ') || 'none'}} ` +
            `(found {${[...actualSet].join(', ') || 'none'}})`,
        );
      }
    }
  }

  return problems;
}

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const packagesDir = path.join(root, 'packages');
const manifests = readdirSync(packagesDir, { withFileTypes: true })
  .filter((entry) => entry.isDirectory())
  .map((entry) => path.join(packagesDir, entry.name, 'package.json'))
  .map((file) => JSON.parse(readFileSync(file, 'utf8')));

const workspaceVersions = Object.fromEntries(
  manifests.filter((manifest) => manifest.name).map((manifest) => [manifest.name, manifest.version]),
);
const expectedDeps = expectedDependencies();
const problems = manifests.flatMap((manifest) => checkManifest(manifest, workspaceVersions, expectedDeps));

if (problems.length > 0) {
  console.error('Publishable-dependency check failed:\n');
  for (const problem of problems) console.error(`  - ${problem}`);
  process.exitCode = 1;
} else {
  console.log(`Publishable-dependency check passed for ${manifests.length} package(s).`);
}
