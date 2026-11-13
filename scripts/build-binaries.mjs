#!/usr/bin/env node
// Compiles the `kafka` and `kafka-studio` bins into standalone executables with
// `bun build --compile`, plus a SHA256SUMS file. Compiles the built `dist/` output (the same code
// npm ships), so build the package and its workspace dependencies first. Needs `bun` on PATH.
//
// Usage: node scripts/build-binaries.mjs <cli|studio> [--target <bun-target>|all] [--outdir <dir>]
// Without --target it builds for the host only.
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { copyFileSync, cpSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

// No `bun-darwin-x64`: Bun 1.4.4 cross-compiles it with an invalid code signature.
export const BINARY_TARGETS = ['bun-linux-x64', 'bun-linux-arm64', 'bun-darwin-arm64', 'bun-windows-x64'];

// `workers` are compiled as extra entrypoints so `new Worker(new URL('./<name>', import.meta.url))`
// finds them next to the entry; `web` is embedded as `web/`, which studio serves relative to its
// own module URL. `external` lists lazy imports that only a development setup can satisfy.
export const BINARY_PACKAGES = {
  cli: { binary: 'kafka', bin: 'dist/bin.js', workers: [], web: null, external: [] },
  studio: {
    binary: 'kafka-studio',
    bin: 'dist/bin.js',
    workers: ['dist/readline-worker.js'],
    web: 'dist/web',
    // KAFKA_STUDIO_DEV's Vite middleware, a devDependency.
    external: ['vite'],
  },
};

/** `bun-darwin-arm64` -> `kafka-darwin-arm64`, with `.exe` for Windows targets. */
export function binaryFileName(binary, target) {
  const name = `${binary}-${target.replace(/^bun-/, '')}`;
  return target.includes('windows') ? `${name}.exe` : name;
}

export function hostTarget() {
  const os = process.platform === 'win32' ? 'windows' : process.platform;
  return `bun-${os}-${process.arch}`;
}

function codecWorkerPath(packageDir) {
  const requireFromPackage = createRequire(path.join(packageDir, 'package.json'));
  const coreEntry = requireFromPackage.resolve('@cookiemonsterdev/kafka-core');
  const workerPath = path.join(path.dirname(coreEntry), 'protocol/compression/codec-worker.js');
  if (!existsSync(workerPath)) {
    throw new Error(`core's codec worker is not at ${workerPath}; build core first, or update this script`);
  }
  return workerPath;
}

/**
 * Lays out the files `bun build --compile` embeds, so they sit at the executable's root
 * (`/$bunfs/root/`): the entry, worker entries, `package.json` (read by `--version`) and studio's
 * `web/` without source maps (the npm tarball excludes them too).
 */
function stageSources(packageDir, spec) {
  const stageDir = mkdtempSync(path.join(tmpdir(), `${spec.binary}-binary-`));
  // Core's codec pool starts `new Worker(<codec-worker module URL>)`. Bundled, that URL is this
  // entry's, so a worker thread must load the codec worker instead of running the bin again.
  // Remove the branch once core resolves its worker from a separate file.
  const entry = [
    "import { isMainThread } from 'node:worker_threads';",
    '',
    `if (isMainThread) await import(${JSON.stringify(path.join(packageDir, spec.bin))});`,
    `else await import(${JSON.stringify(codecWorkerPath(packageDir))});`,
    '',
  ].join('\n');
  writeFileSync(path.join(stageDir, 'entry.mjs'), entry);
  for (const worker of spec.workers) {
    writeFileSync(
      path.join(stageDir, path.basename(worker)),
      `import ${JSON.stringify(path.join(packageDir, worker))};\n`,
    );
  }
  copyFileSync(path.join(packageDir, 'package.json'), path.join(stageDir, 'package.json'));
  if (spec.web !== null) {
    cpSync(path.join(packageDir, spec.web), path.join(stageDir, 'web'), {
      recursive: true,
      filter: (source) => !source.endsWith('.map'),
    });
  }
  return stageDir;
}

function compile(stageDir, spec, target, outfile) {
  const args = [
    'build',
    '--compile',
    `--target=${target}`,
    './entry.mjs',
    ...spec.workers.map((worker) => `./${path.basename(worker)}`),
    '--asset=package.json',
    ...(spec.web === null ? [] : ['--asset=web']),
    ...spec.external.map((specifier) => `--external=${specifier}`),
    // The CLI loads `.env` itself; Bun's autoload would also read `.env.local`/`.env.production`,
    // and a `bunfig.toml` in the working directory could preload code into the executable.
    '--no-compile-autoload-dotenv',
    '--no-compile-autoload-bunfig',
    `--outfile=${outfile}`,
  ];
  const result = spawnSync('bun', args, { cwd: stageDir, stdio: 'inherit' });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`bun build --compile for ${target} exited with ${String(result.status)}`);
  }
}

function sha256(file) {
  return createHash('sha256').update(readFileSync(file)).digest('hex');
}

function main() {
  const { values, positionals } = parseArgs({
    allowPositionals: true,
    options: {
      target: { type: 'string', multiple: true },
      outdir: { type: 'string', default: path.join(ROOT, 'dist', 'binaries') },
    },
  });
  const packageName = positionals[0];
  const spec = BINARY_PACKAGES[packageName];
  if (spec === undefined || positionals.length !== 1) {
    console.error(
      `Usage: node scripts/build-binaries.mjs <${Object.keys(BINARY_PACKAGES).join('|')}> ` +
        '[--target <bun-target>|all] [--outdir <dir>]',
    );
    process.exit(1);
  }
  const requested = values.target ?? [hostTarget()];
  const targets = requested.includes('all') ? BINARY_TARGETS : requested;
  const outdir = path.resolve(values.outdir);
  const packageDir = path.join(ROOT, 'packages', packageName);

  mkdirSync(outdir, { recursive: true });
  const stageDir = stageSources(packageDir, spec);
  try {
    const sums = targets.map((target) => {
      const fileName = binaryFileName(spec.binary, target);
      compile(stageDir, spec, target, path.join(outdir, fileName));
      return `${sha256(path.join(outdir, fileName))}  ${fileName}\n`;
    });
    writeFileSync(path.join(outdir, `${spec.binary}-SHA256SUMS`), sums.join(''));
  } finally {
    rmSync(stageDir, { recursive: true, force: true });
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main();
}
