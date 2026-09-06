import { platform } from 'node:os';
import type * as StudioModule from '@cookiemonsterdev/kafka-studio';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';

/**
 * Not a string literal at the `import()` call site below — see there for why. Kept in one place so
 * the install hint can never drift from the specifier actually being resolved.
 */
const STUDIO_PACKAGE = '@cookiemonsterdev/kafka-studio';

const MODULE_NOT_FOUND_CODES = new Set(['ERR_MODULE_NOT_FOUND', 'MODULE_NOT_FOUND']);

/**
 * Checked against `error` itself and, once, its `.cause` — some loaders (and this command's own
 * test suite, which mocks the dynamic import) wrap the original resolution error rather than
 * throwing it directly, so the code the caller actually needs to see would otherwise be hidden a
 * layer down.
 */
function isModuleNotFoundError(error: unknown): boolean {
  const codeOf = (value: unknown): unknown =>
    typeof value === 'object' && value !== null ? (value as { code?: unknown }).code : undefined;
  if (typeof error !== 'object' || error === null) return false;
  if (MODULE_NOT_FOUND_CODES.has(codeOf(error) as string)) return true;
  return MODULE_NOT_FOUND_CODES.has(codeOf((error as { cause?: unknown }).cause) as string);
}

function waitForAbort(signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => signal.addEventListener('abort', () => resolve(), { once: true }));
}

export const studioCommand: CommandSpec = {
  path: ['studio'],
  summary: 'Launch the local web UI for browsing and driving the cluster',
  flags: [
    { name: 'port', type: 'number', alias: 'p', brief: 'port to listen on (default: first free port in 5757-5807)' },
    { name: 'host', type: 'string', brief: 'address to bind (default: 127.0.0.1)' },
    { name: 'browser', type: 'string', brief: 'browser command to open, or "none" to disable' },
    { name: 'read-only', type: 'boolean', brief: 'reject mutating API requests with 403' },
  ],
  examples: ['studio', 'studio --read-only', 'studio --port 5757 --browser none'],
  // Runtime resolution keeps the studio out of this package's own dependency set (tarball size, no
  // publish-order coupling) — see `@cookiemonsterdev/kafka-studio`'s own package layout notes.
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ runtime, flags }) {
    if (typeof flags.port === 'number' && !Number.isInteger(flags.port)) {
      runtime.stderr.write(`kafka studio: --port must be an integer, got "${String(flags.port)}"\n`);
      return EXIT_CODES.usage;
    }

    let studio: typeof StudioModule;
    try {
      // A non-literal specifier: `@cookiemonsterdev/kafka-studio` is a devDependency of this
      // package (types only, via the `import type` above) and never a runtime one, so a literal
      // `import('@cookiemonsterdev/kafka-studio')` here would make Rollup try to resolve and bundle
      // it at build time — and fail, since it isn't installed as a real dependency. A variable
      // specifier is opaque to bundlers, leaving a genuine runtime `import()` that only resolves
      // (or doesn't) the moment this command actually runs.
      studio = (await import(STUDIO_PACKAGE)) as typeof StudioModule;
    } catch (error) {
      if (!isModuleNotFoundError(error)) throw error;
      runtime.stderr.write(
        `kafka studio: "${STUDIO_PACKAGE}" is not installed.\n` +
          `Install it alongside the CLI to use this command:\n\n` +
          `  npm install --global ${STUDIO_PACKAGE}\n`,
      );
      return EXIT_CODES.operationFailed;
    }

    const handle = await studio.startStudio(
      {
        ...(typeof flags.port === 'number' ? { port: flags.port } : {}),
        ...(typeof flags.host === 'string' ? { host: flags.host } : {}),
        ...(typeof flags.browser === 'string' ? { browser: flags.browser } : {}),
        readOnly: flags['read-only'] === true,
      },
      {
        argv: runtime.argv,
        cwd: runtime.cwd,
        env: runtime.env,
        // `node:os`'s `platform()`, not the global `process.platform` — this file is covered by
        // the same `no-restricted-globals` rule as the rest of this package's commands.
        platform: platform(),
        stdout: runtime.stdout,
        stderr: runtime.stderr,
        now: () => runtime.now(),
        exit: (code: number) => runtime.exit(code),
        signal: runtime.signal,
      },
    );

    // Blocks until Ctrl-C/SIGTERM, same as `kafka-studio` run standalone — `main.ts` re-checks
    // `runtime.signal` once this resolves and reports the interruption as its own exit code, so
    // returning `ok` here is what a clean shutdown reports as.
    await waitForAbort(runtime.signal);
    await handle.stop();

    return EXIT_CODES.ok;
  },
};
