import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { COMPLETION_SHELLS, isCompletionShell, renderCompletionScript } from '../../completion/scripts';
import { EXIT_CODES } from '../../errors/exit-codes';

const PROGRAM_NAME = 'kafka';

export const completionCommand: CommandSpec = {
  path: ['completion'],
  summary: 'Print a shell completion script for bash, zsh, or fish',
  positionals: [{ name: 'shell', brief: `the shell to generate a script for (${COMPLETION_SHELLS.join('|')})` }],
  examples: ['completion bash', 'completion zsh', 'completion fish'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.usage],
  async run({ positionals, output }) {
    const [shell] = positionals;
    if (shell === undefined) {
      throw new CliUsageError(`completion requires a shell argument (one of ${COMPLETION_SHELLS.join(', ')})`);
    }
    if (!isCompletionShell(shell)) {
      throw new CliUsageError(`unknown shell "${shell}" — expected one of ${COMPLETION_SHELLS.join(', ')}`);
    }

    const script = renderCompletionScript(shell, PROGRAM_NAME);
    output.write({ human: () => script.trimEnd(), json: () => script.trimEnd() });
    return EXIT_CODES.ok;
  },
};
