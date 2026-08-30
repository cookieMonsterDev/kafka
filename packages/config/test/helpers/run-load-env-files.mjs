/**
 * Subprocess driver for `loadEnvFiles` tests. `process.loadEnvFile` mutates the real `process.env`
 * with no way to undo it, so exercising it must happen in a fresh process per test rather than
 * inside a shared vitest worker.
 *
 * Usage: `node run-load-env-files.mjs <cwd> <file1> [file2...]`. Prints one JSON line:
 * `{ loaded, missing, env }`, where `env` is every `process.env` entry whose key starts with
 * `KAFKA_CONFIG_TEST_` (the prefix every fixture `.env` file in these tests uses, so the driver
 * never has to know a test's variable names ahead of time).
 */
const { loadEnvFiles } = await import('../../src/load-env-files.ts');

const [, , cwd, ...files] = process.argv;
if (cwd == null) {
  throw new Error('Usage: run-load-env-files.mjs <cwd> <file1> [file2...]');
}

const result = loadEnvFiles({ cwd, files: files.length > 0 ? files : undefined });

const env = {};
for (const [key, value] of Object.entries(process.env)) {
  if (key.startsWith('KAFKA_CONFIG_TEST_')) env[key] = value;
}

console.log(JSON.stringify({ ...result, env }));
