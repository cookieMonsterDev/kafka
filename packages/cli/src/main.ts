import { dispatch } from './dispatch';
import { EXIT_CODES } from './errors/exit-codes';
import type { Runtime } from './runtime';

function exitCodeForSignal(reason: unknown): number {
  if (reason === 'SIGTERM') return 143;
  return EXIT_CODES.aborted;
}

/**
 * The whole program as a pure function of a {@link Runtime}: parse `argv`, route to a command,
 * run it, and resolve to an exit code. Never throws — every failure, expected or not, is caught
 * and turned into a code by the dispatcher's own error mapping, down to a genuine unexpected bug,
 * which becomes the internal-bug code with a bug-report line rather than an uncaught rejection.
 *
 * dispatch is imported statically (not lazily): every invocation runs it regardless, including
 * --help/--version, so splitting it into a second chunk would only add a module-resolution step
 * for no benefit. Core itself stays the one thing loaded lazily — see admin/open.ts.
 */
export async function main(runtime: Runtime): Promise<number> {
  if (runtime.signal.aborted) {
    return exitCodeForSignal(runtime.signal.reason);
  }

  const code = await dispatch(runtime);

  if (runtime.signal.aborted) {
    // A signal landed mid-command; report the interruption over whatever the command's own
    // result was, matching how a shell reports a killed foreground process.
    return exitCodeForSignal(runtime.signal.reason);
  }

  return code;
}
