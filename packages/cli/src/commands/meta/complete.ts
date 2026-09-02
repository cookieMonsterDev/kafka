import type { CommandSpec } from '../../args/define';
import { getCompletions } from '../../completion/complete';
import { EXIT_CODES } from '../../errors/exit-codes';

/**
 * Takes a thunk rather than the command list itself: `commands/index.ts` is what mounts this very
 * command, so it can't hand over its own `ALL_COMMANDS` constant until that array literal has
 * finished evaluating. A thunk defers the read to call time — by which point the module has long
 * since finished loading — without an `await import()` that would otherwise force this package's
 * single-file bundle apart into an extra chunk just to break an import cycle that a closure
 * already breaks for free.
 *
 * Not registered with a JSON rendering of its own on purpose: a shell reads this command's
 * stdout as one candidate per line, in whatever format the invocation resolved to, so `human`
 * and `json` intentionally produce the same plain list rather than a JSON document — a shell
 * completion function has no JSON parser wired up to consume one.
 */
export function createCompleteCommand(getCommands: () => readonly CommandSpec[]): CommandSpec {
  return {
    path: ['complete'],
    summary: 'List completion candidates for a partial command line (used by the shell scripts, not by hand)',
    hidden: true,
    positionals: [
      { name: 'words', variadic: true, brief: 'every word typed so far, including the one being completed' },
    ],
    exitCodes: [EXIT_CODES.ok],
    async run({ positionals, output }) {
      const suggestions = getCompletions(getCommands(), positionals);
      const rendered = suggestions.join('\n');
      output.write({ human: () => rendered, json: () => rendered });
      return EXIT_CODES.ok;
    },
  };
}
