#!/usr/bin/env node
// Smoke-tests the built packages (`dist/`) on whichever runtime runs this script — `bun` in CI's
// Bun job, `node` anywhere else. Vitest transforms every import itself, so a unit suite can pass on
// a runtime that still fails to link the published output (a missing named export, an unsupported
// `node:` API, a worker that never lets the process exit). This loads that output exactly the way
// a consumer would.
//
// Needs every package built first, in order: config, core, studio, cli.
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const PACKAGES_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../packages');
const SCRIPT_PATH = fileURLToPath(import.meta.url);
const CODEC_CHILD_FLAG = '--codec-child';
const CHILD_TIMEOUT_MS = 30_000;

function distPath(pkg, file) {
  return path.join(PACKAGES_DIR, pkg, 'dist', file);
}

function importDist(pkg, file = 'index.js') {
  return import(pathToFileURL(distPath(pkg, file)).href);
}

function packageVersion(pkg) {
  return JSON.parse(readFileSync(path.join(PACKAGES_DIR, pkg, 'package.json'), 'utf8')).version;
}

function assertExports(mod, names, pkg) {
  for (const name of names) {
    assert.ok(mod[name] != null, `${pkg} is missing the "${name}" export`);
  }
}

function run(args) {
  const command = args.join(' ');
  const result = spawnSync(process.execPath, args, { encoding: 'utf8', timeout: CHILD_TIMEOUT_MS });
  const output = `stdout:\n${result.stdout}\nstderr:\n${result.stderr}`;
  // spawnSync kills a child that outlives `timeout` and reports ETIMEDOUT: it never exited on its own.
  assert.notEqual(result.error?.code, 'ETIMEDOUT', `"${command}" did not exit within ${CHILD_TIMEOUT_MS}ms\n${output}`);
  if (result.error) throw result.error;
  assert.equal(result.signal, null, `"${command}" was killed by ${result.signal}\n${output}`);
  assert.equal(result.status, 0, `"${command}" exited with ${result.status}\n${output}`);
  return result.stdout;
}

/**
 * Runs in a child process: a codec round trip through core's built off-thread pool, without
 * stopping the pool, so the child only exits if its idle workers leave the event loop free.
 */
async function codecChild() {
  const { runCodecOp } = await import(pathToFileURL(distPath('core', 'protocol/compression/off-thread.js')).href);
  for (const codec of ['snappy', 'lz4']) {
    const input = Buffer.from(`bun-smoke ${codec} `.repeat(64));
    const compressed = await runCodecOp(`${codec}-compress`, input);
    const output = await runCodecOp(`${codec}-decompress`, compressed);
    assert.ok(output.equals(input), `${codec} round trip through the worker pool changed the bytes`);
  }
  console.log('codec round trip ok');
}

const checks = [
  [
    'config exports',
    async () => {
      const config = await importDist('config');
      assertExports(
        config,
        ['loadConfigFileAsync', 'loadConfigFileSync', 'discoverConfigFile', 'defaultOnConfigDiagnostic'],
        'config',
      );
    },
  ],
  [
    'core exports',
    async () => {
      const core = await importDist('core');
      assertExports(
        core,
        ['Kafka', 'CompressionTypes', 'CompressionCodecs', 'KafkaError', 'resolveKafkaConfigAsync'],
        'core',
      );
      assert.equal(typeof core.Kafka.fromConfig, 'function', 'core is missing Kafka.fromConfig');
    },
  ],
  [
    'studio exports',
    async () => {
      assertExports(await importDist('studio'), ['startStudio'], 'studio');
    },
  ],
  [
    'cli module loads',
    async () => {
      // The CLI's library entry is intentionally empty (`export {}`); its surface is the bin below.
      await importDist('cli');
    },
  ],
  [
    'cli --version',
    async () => {
      const stdout = run([distPath('cli', 'bin.js'), '--version']);
      assert.ok(stdout.includes(packageVersion('cli')), `cli --version printed ${JSON.stringify(stdout)}`);
    },
  ],
  [
    'studio --help',
    async () => {
      const stdout = run([distPath('studio', 'bin.js'), '--help']);
      assert.ok(stdout.includes('Usage: kafka-studio'), `studio --help printed ${JSON.stringify(stdout)}`);
    },
  ],
  [
    'codec worker pool round trip, then exits on its own',
    async () => {
      run([SCRIPT_PATH, CODEC_CHILD_FLAG]);
    },
  ],
  [
    'TypeScript config file loads',
    async () => {
      const { loadConfigFileAsync } = await importDist('config');
      const { Kafka } = await importDist('core');
      const dir = mkdtempSync(path.join(tmpdir(), 'kafka-bun-smoke-'));
      try {
        const file = path.join(dir, 'kafka.config.ts');
        writeFileSync(
          file,
          [
            'interface SmokeConfig { client: { brokers: string[] } }',
            "const config: SmokeConfig = { client: { brokers: ['smoke:9092'] } };",
            'export default config;',
            '',
          ].join('\n'),
        );
        assert.deepEqual(await loadConfigFileAsync(file), { client: { brokers: ['smoke:9092'] } });
        assert.ok(
          (await Kafka.fromConfig({}, { cwd: dir })) instanceof Kafka,
          'Kafka.fromConfig did not build a Kafka',
        );
      } finally {
        rmSync(dir, { recursive: true, force: true });
      }
    },
  ],
];

async function main() {
  const runtime = typeof process.versions.bun === 'string' ? `bun ${process.versions.bun}` : `node ${process.version}`;
  console.log(`Smoke-testing built packages on ${runtime}`);

  let failed = 0;
  for (const [name, check] of checks) {
    try {
      await check();
      console.log(`ok - ${name}`);
    } catch (error) {
      failed += 1;
      console.error(`not ok - ${name}`);
      console.error(error);
    }
  }

  if (failed > 0) {
    console.error(`${failed} of ${checks.length} smoke checks failed`);
    process.exitCode = 1;
  }
}

if (process.argv[2] === CODEC_CHILD_FLAG) {
  await codecChild();
} else {
  await main();
}
