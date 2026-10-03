/**
 * Subprocess driver for `loadConfigFileAsync`'s error messages. In-process, vitest routes
 * `import()` through Vite's own transform, which strips enums and resolves extensionless imports
 * itself — so only a real process shows what the runtime's own `import()` does with a fixture.
 * Runtime-agnostic: run it with `node` or `bun` (`process.execPath`).
 *
 * Usage: `<runtime> run-load-async.mjs <configPath>`. Prints one JSON line to stdout:
 * `{ ok, config | (name, tag, message) }`.
 */
import './register-src-resolve.mjs';

const { loadConfigFileAsync } = await import('../../src/load-async.ts');

const [, , configPath] = process.argv;
if (configPath == null) {
  throw new Error('Usage: run-load-async.mjs <configPath>');
}

try {
  const config = await loadConfigFileAsync(configPath);
  console.log(JSON.stringify({ ok: true, config }));
} catch (error) {
  console.log(
    JSON.stringify({
      ok: false,
      name: error instanceof Error ? error.constructor.name : typeof error,
      tag: error?.tag,
      message: error instanceof Error ? error.message : String(error),
    }),
  );
}
