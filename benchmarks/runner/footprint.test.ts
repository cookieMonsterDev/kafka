import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, describe, it } from 'node:test';

import { footprintScript, parseFootprints } from './footprint.ts';

const installRoot = await mkdtemp(join(tmpdir(), 'kafka-bench-footprint-'));

after(async () => {
  await rm(installRoot, { recursive: true, force: true });
});

/** Writes a package with the given manifest fields and files, and returns the bytes of its files, manifest included. */
async function writePackage(
  packageDir: string,
  manifest: Record<string, unknown>,
  files: Record<string, string>,
): Promise<number> {
  await mkdir(packageDir, { recursive: true });
  const manifestText = JSON.stringify({ version: '1.0.0', ...manifest });
  await writeFile(join(packageDir, 'package.json'), manifestText);
  let bytes = Buffer.byteLength(manifestText);
  for (const [name, content] of Object.entries(files)) {
    await mkdir(join(packageDir, name, '..'), { recursive: true });
    await writeFile(join(packageDir, name), content);
    bytes += Buffer.byteLength(content);
  }
  return bytes;
}

function runFootprintScript(packages: Record<string, string>): string {
  return execFileSync(process.execPath, ['-e', footprintScript, installRoot, JSON.stringify(packages)], {
    encoding: 'utf8',
  });
}

describe('footprintScript', () => {
  it('splits the closure into required and optional parts', async () => {
    const modules = join(installRoot, 'node_modules');
    const clientDir = join(modules, 'client');
    let requiredBytes = await writePackage(
      clientDir,
      {
        version: '2.0.0',
        // optional-dep is listed in both; as in npm, optionalDependencies wins.
        dependencies: { 'required-dep': '*', nested: '*', 'optional-dep': '*' },
        optionalDependencies: { 'optional-dep': '*', 'missing-optional': '*' },
      },
      { 'index.js': 'module.exports = 1;\n', 'lib/util.js': 'exports.util = 2;\n' },
    );
    // Resolved from the client's own node_modules first, and counted once, not while sizing the client.
    requiredBytes += await writePackage(join(clientDir, 'node_modules', 'nested'), {}, { 'index.js': 'nested' });
    requiredBytes += await writePackage(join(modules, 'required-dep'), { dependencies: { shared: '*' } }, {});
    requiredBytes += await writePackage(join(modules, 'shared'), {}, { 'index.js': 'shared' });
    let optionalBytes = await writePackage(
      join(modules, 'optional-dep'),
      { dependencies: { shared: '*', 'optional-child': '*' } },
      { 'addon.node': 'binary' },
    );
    optionalBytes += await writePackage(join(modules, 'optional-child'), {}, { 'index.js': 'child' });

    const [client, absent] = JSON.parse(runFootprintScript({ client: 'client', absent: 'absent' })) as unknown[];

    assert.deepEqual(client, {
      client: 'client',
      packageName: 'client',
      version: '2.0.0',
      // client, nested, required-dep and shared; shared is also reached optionally but stays required.
      required: { bytes: requiredBytes, fileCount: 8, dependencyCount: 3, hasNativeAddon: false },
      optional: { bytes: optionalBytes, fileCount: 4, dependencyCount: 2, hasNativeAddon: true },
    });
    assert.deepEqual(absent, {
      client: 'absent',
      packageName: 'absent',
      version: null,
      required: null,
      optional: null,
    });
  });
});

describe('parseFootprints', () => {
  const part = { bytes: 10, fileCount: 2, dependencyCount: 1, hasNativeAddon: false };

  it('returns one footprint per client and nulls both parts when a package is not installed', () => {
    const output = JSON.stringify([
      { client: 'kafka-core', version: '2.8.1', required: part, optional: { ...part, hasNativeAddon: true } },
      { client: 'platformatic', version: '2.15.0', required: part, optional: part },
      { client: 'kafkajs', version: '2.2.4', required: part, optional: part },
      { client: 'confluent', version: null, required: null, optional: null },
    ]);

    const footprints = parseFootprints(`install log\n${output}\n`, 'image');

    assert.deepEqual(
      footprints.map((footprint) => footprint.client),
      ['kafka-core', 'platformatic', 'kafkajs', 'confluent'],
    );
    assert.deepEqual(footprints[0], {
      client: 'kafka-core',
      packageName: '@cookiemonsterdev/kafka-core',
      version: '2.8.1',
      required: part,
      optional: { ...part, hasNativeAddon: true },
    });
    assert.equal(footprints[3]?.required, null);
    assert.equal(footprints[3]?.optional, null);
  });

  it('rejects output that is not JSON or misses a client', () => {
    assert.throws(() => parseFootprints('not json', 'image'), /not JSON/);
    assert.throws(() => parseFootprints('[]', 'image'), /has no entry for kafka-core/);
  });
});
