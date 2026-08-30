import { EXIT_CODES } from './errors/exit-codes';
import type { Runtime } from './runtime';

function exitCodeForSignal(reason: unknown): number {
  if (reason === 'SIGTERM') return 143;
  return EXIT_CODES.aborted;
}

/**
 * Command routing lands with the argument parser; until then every invocation is a usage error
 * rather than a silent no-op, so `main`'s contract (always resolves to a code) is real from the
 * first commit.
 */
async function route(runtime: Runtime): Promise<number> {
  runtime.stderr.write('kafka: no commands are available yet\n');
  return EXIT_CODES.usage;
}

/**
 * The whole program as a pure function of a {@link Runtime}: parse `argv`, route to a command,
 * run it, and resolve to an exit code. Never throws for an ordinary failure — every expected
 * failure is caught and turned into a code; only a genuine bug propagates, and `bin.ts` maps that
 * to the internal-bug code before it ever reaches a real process.
 */
export async function main(runtime: Runtime): Promise<number> {
  if (runtime.signal.aborted) {
    return exitCodeForSignal(runtime.signal.reason);
  }

  const code = await route(runtime);

  if (runtime.signal.aborted) {
    // A signal landed mid-command; report the interruption over whatever the command's own
    // result was, matching how a shell reports a killed foreground process.
    return exitCodeForSignal(runtime.signal.reason);
  }

  return code;
}
