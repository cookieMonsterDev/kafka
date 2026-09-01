import { CliAbortedError } from '../errors/aborted-error';
import type { Runtime } from '../runtime';

const YES_PATTERN = /^y(es)?$/i;

/** The slice of `Runtime` a confirmation prompt needs — narrow so it stays fake-able in tests. */
export type ConfirmRuntime = Pick<Runtime, 'isTty' | 'env' | 'stdin' | 'stderr'>;

function isNonInteractive(runtime: ConfirmRuntime): boolean {
  return !runtime.isTty || runtime.env.CI === 'true';
}

/** Reads one line from `stdin`, without pulling in `node:readline`'s stream-interface contract. */
function readLine(stdin: Runtime['stdin']): Promise<string> {
  return new Promise((resolve) => {
    let buffer = '';
    stdin.setEncoding('utf8');
    stdin.on('data', (chunk) => {
      buffer += chunk;
      const newlineIndex = buffer.indexOf('\n');
      if (newlineIndex !== -1) resolve(buffer.slice(0, newlineIndex));
    });
    stdin.on('end', () => resolve(buffer));
  });
}

export interface ConfirmDestructiveInput {
  readonly runtime: ConfirmRuntime;
  /** `--yes` — explicit consent, skips the prompt entirely. */
  readonly yes: boolean;
  /** What to ask before proceeding, e.g. `"Delete topic \"orders\"?"` — never includes the `[y/N]` suffix. */
  readonly message: string;
  /** `cli.confirmDestructive` — `false` waives this tier's prompt/`--yes` requirement outright. */
  readonly confirmDestructive?: boolean;
}

/**
 * Gates a destructive command behind either `--yes` or an interactive y/N prompt (asked on
 * stderr, per the CLI's stdout-is-data contract). Off a TTY, or under `CI=true`, there is no
 * prompt to answer, so `--yes` becomes mandatory. `cli.confirmDestructive: false` waives this
 * tier entirely — but never the independent `--force` tier a command enforces itself via
 * {@link requireForce}, which this function knows nothing about.
 */
export async function confirmDestructive(input: ConfirmDestructiveInput): Promise<void> {
  if (input.confirmDestructive === false) return;
  if (input.yes) return;

  if (isNonInteractive(input.runtime)) {
    throw new CliAbortedError(`${input.message} requires --yes to run without an interactive prompt`);
  }

  input.runtime.stderr.write(`${input.message} [y/N] `);
  const answer = await readLine(input.runtime.stdin);
  if (!YES_PATTERN.test(answer.trim())) {
    throw new CliAbortedError('aborted: not confirmed');
  }
}

export interface RequireForceInput {
  /** `--force`. */
  readonly force: boolean;
  /** What safety check `--force` overrides, e.g. `"deleting 12 topics in one call"`. */
  readonly reason: string;
}

/**
 * Enforces a `--force`-gated safety check (an internal topic, a large batch, an unclean
 * election, …) — independent of `--yes`/`confirmDestructive`, and never waived by either.
 */
export function requireForce(input: RequireForceInput): void {
  if (!input.force) {
    throw new CliAbortedError(`${input.reason} requires --force`);
  }
}
