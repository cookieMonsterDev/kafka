#!/usr/bin/env node
// Smoke-tests a standalone executable from scripts/build-binaries.mjs on the platform it targets,
// without installing anything: the executable must carry its own runtime, assets and workers.
//
// Usage: node scripts/smoke-binaries.mjs <cli|studio> <dir>
// Runs `<dir>/<binary>-<host target>`, e.g. `kafka-linux-x64`.
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { once } from 'node:events';
import { chmodSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { BINARY_PACKAGES, binaryFileName, hostTarget } from './build-binaries.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const CHILD_TIMEOUT_MS = 90_000;

function packageVersion(packageName) {
  return JSON.parse(readFileSync(path.join(ROOT, 'packages', packageName, 'package.json'), 'utf8')).version;
}

function run(binary, args, options = {}) {
  const result = spawnSync(binary, args, { encoding: 'utf8', timeout: CHILD_TIMEOUT_MS, ...options });
  if (result.error) throw result.error;
  return result;
}

async function withTempDir(files, check) {
  const dir = mkdtempSync(path.join(tmpdir(), 'kafka-binary-smoke-'));
  try {
    for (const [name, content] of Object.entries(files)) writeFileSync(path.join(dir, name), content);
    return await check(dir);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

function assertVersionAndHelp(binary, packageName, usage) {
  const version = run(binary, ['--version']);
  assert.equal(version.status, 0, `--version exited with ${version.status}: ${version.stderr}`);
  assert.equal(version.stdout.trim(), packageVersion(packageName));
  const help = run(binary, ['--help']);
  assert.equal(help.status, 0, `--help exited with ${help.status}: ${help.stderr}`);
  assert.ok(help.stdout.includes(usage), `--help printed ${JSON.stringify(help.stdout)}`);
}

const cliChecks = [
  ['--version and --help', (binary) => assertVersionAndHelp(binary, 'cli', 'Usage: kafka <command>')],
  [
    'loads kafka.config.ts and ignores Bun-only .env.local and bunfig.toml',
    (binary) =>
      withTempDir(
        {
          'kafka.config.ts': "const brokers: string[] = ['from-file:9092'];\nexport default { client: { brokers } };\n",
          '.env.local': 'KAFKA_BROKERS=from-env-local:9092\n',
          'bunfig.toml': 'preload = ["./preload.ts"]\n',
          'preload.ts': "console.log('bunfig preload ran');\n",
        },
        (dir) => {
          const result = run(binary, ['doctor', '--json'], { cwd: dir });
          assert.equal(result.status, 0, `doctor exited with ${result.status}: ${result.stderr}`);
          assert.ok(!result.stdout.includes('bunfig preload ran'), 'bunfig.toml preload ran');
          const report = JSON.parse(result.stdout);
          assert.equal(path.basename(report.configPath), 'kafka.config.ts');
          assert.equal(report.configSource.keys.brokers, 'file', 'brokers came from somewhere other than the file');
        },
      ),
  ],
  [
    'ping fails cleanly without a broker',
    (binary) => {
      const result = run(binary, ['ping', '--brokers', '127.0.0.1:1', '--quiet']);
      assert.equal(result.status, 1, `ping exited with ${result.status}: ${result.stderr}`);
      assert.match(result.stderr, /ECONNREFUSED/);
    },
  ],
];

/** Resolves with the base URL and session token from studio's `listening on <url>#token=<token>` banner. */
async function waitForBanner(child) {
  let output = '';
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`studio did not start:\n${output}`)), CHILD_TIMEOUT_MS);
    child.stdout.on('data', (chunk) => {
      output += chunk;
      const match = /listening on (http:\/\/\S+?)\/#token=([0-9a-f]+)/.exec(output);
      if (match !== null) {
        clearTimeout(timer);
        resolve({ baseUrl: match[1], token: match[2] });
      }
    });
    child.stderr.on('data', (chunk) => {
      output += chunk;
    });
    child.once('exit', (code) => reject(new Error(`studio exited with ${String(code)}:\n${output}`)));
  });
}

async function firstTerminalOutput(baseUrl, token) {
  const headers = { 'x-kafka-studio-token': token };
  const created = await fetch(`${baseUrl}/api/terminal/sessions`, { method: 'POST', headers });
  assert.equal(created.status, 201, 'creating a terminal session failed');
  const { id } = await created.json();
  const events = await fetch(`${baseUrl}/api/terminal/sessions/${id}/events`, {
    headers,
    signal: AbortSignal.timeout(CHILD_TIMEOUT_MS),
  });
  const reader = events.body.getReader();
  const { value } = await reader.read();
  await reader.cancel();
  return Buffer.from(value).toString('utf8');
}

const studioChecks = [
  ['--version and --help', (binary) => assertVersionAndHelp(binary, 'studio', 'Usage: kafka-studio')],
  [
    'serves the embedded web UI and starts the terminal worker',
    (binary) =>
      withTempDir({ 'kafka.config.json': '{"client":{"brokers":["127.0.0.1:1"]}}' }, async (dir) => {
        const child = spawn(binary, ['--browser', 'none'], { cwd: dir });
        child.stdout.setEncoding('utf8');
        child.stderr.setEncoding('utf8');
        const exited = once(child, 'exit');
        try {
          const { baseUrl, token } = await waitForBanner(child);
          const index = await fetch(`${baseUrl}/`, { signal: AbortSignal.timeout(CHILD_TIMEOUT_MS) });
          assert.equal(index.status, 200, 'GET / failed');
          const html = await index.text();
          const asset = /src="(\/assets\/[^"]+\.js)"/.exec(html)?.[1];
          assert.ok(asset, `index.html names no script asset:\n${html}`);
          const script = await fetch(`${baseUrl}${asset}`, { signal: AbortSignal.timeout(CHILD_TIMEOUT_MS) });
          assert.equal(script.status, 200, `GET ${asset} failed`);
          assert.match(await firstTerminalOutput(baseUrl, token), /event: output/);
        } finally {
          child.kill('SIGINT');
          const forceKill = setTimeout(() => child.kill('SIGKILL'), 10_000);
          await exited;
          clearTimeout(forceKill);
        }
      }),
  ],
];

async function main() {
  const [packageName, dir] = process.argv.slice(2);
  const spec = BINARY_PACKAGES[packageName];
  if (spec === undefined || dir === undefined) {
    console.error(`Usage: node scripts/smoke-binaries.mjs <${Object.keys(BINARY_PACKAGES).join('|')}> <dir>`);
    process.exit(1);
  }
  const binary = path.resolve(dir, binaryFileName(spec.binary, hostTarget()));
  // Release artifacts lose their executable bit in transit.
  if (process.platform !== 'win32') chmodSync(binary, 0o755);
  console.log(`Smoke-testing ${binary}`);

  let failed = 0;
  const checks = packageName === 'cli' ? cliChecks : studioChecks;
  for (const [name, check] of checks) {
    try {
      await check(binary);
      console.log(`ok - ${name}`);
    } catch (error) {
      failed += 1;
      console.error(`not ok - ${name}`);
      console.error(error);
    }
  }
  if (failed > 0) {
    console.error(`${failed} of ${checks.length} binary smoke checks failed`);
    process.exitCode = 1;
  }
}

await main();
