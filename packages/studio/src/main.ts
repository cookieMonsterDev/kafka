import { StudioUsageError, parseStudioArgs, usageText } from './args';
import { EXIT_CODES, exitCodeForSignal } from './exit-codes';
import { startStudio } from './index';
import type { Runtime } from './runtime';
import { readOwnVersion } from './version';

function waitForAbort(signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => signal.addEventListener('abort', () => resolve(), { once: true }));
}

/**
 * The whole program as a pure function of a {@link Runtime}: parse `argv`, either print help/the
 * version and return immediately, or start the server and block until `SIGINT`/`SIGTERM`, then
 * shut it down cleanly. Never throws for anything a user could cause — only a genuine bug escapes.
 */
export async function main(runtime: Runtime): Promise<number> {
  if (runtime.signal.aborted) return exitCodeForSignal(runtime.signal.reason);

  let options;
  try {
    options = parseStudioArgs(runtime.argv);
  } catch (error) {
    if (error instanceof StudioUsageError) {
      runtime.stderr.write(`kafka-studio: ${error.message}\n`);
      return EXIT_CODES.usage;
    }
    throw error;
  }

  if (options.help) {
    runtime.stdout.write(usageText());
    return EXIT_CODES.ok;
  }

  if (options.version) {
    runtime.stdout.write(`${readOwnVersion(import.meta.url)}\n`);
    return EXIT_CODES.ok;
  }

  const studio = await startStudio(options, runtime);
  await waitForAbort(runtime.signal);
  await studio.stop();

  return exitCodeForSignal(runtime.signal.reason);
}
