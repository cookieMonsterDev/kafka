import { CliUsageError } from './args/coerce';
import type { CommandSpec } from './args/define';
import { extractGlobalFlags } from './args/pre-parse';
import { parseCommandArgs } from './args/parse';
import { ALL_COMMANDS } from './commands/index';
import { runHelpCommand } from './commands/meta/help';
import { runVersionCommand } from './commands/meta/version';
import { shouldUseColor } from './output/colors';
import { createCommandOutput, resolveOutputFormat } from './output/format';
import { verbosityToLogLevel } from './output/logger';
import { EXIT_CODES } from './errors/exit-codes';
import type { HelpRenderOptions } from './help/render';
import { commandGroups, createRegistry } from './registry';
import type { Runtime } from './runtime';

const HELP_OPTIONS: HelpRenderOptions = { programName: 'kafka' };

/** The longest registered command path that `argv` starts with, stopping at the first flag. */
function resolveCommandPath(
  registry: ReadonlyMap<string, CommandSpec>,
  argv: readonly string[],
): { path: string[]; rest: string[] } {
  const tokens: string[] = [];
  let matchedLength = 0;
  for (const token of argv) {
    if (token.startsWith('-')) break;
    tokens.push(token);
    if (registry.has(tokens.join(' '))) matchedLength = tokens.length;
  }
  return { path: tokens.slice(0, matchedLength), rest: argv.slice(matchedLength) };
}

/** Every leading non-flag token, regardless of whether it resolves to a registered command. */
function attemptedPath(argv: readonly string[]): string[] {
  const tokens: string[] = [];
  for (const token of argv) {
    if (token.startsWith('-')) break;
    tokens.push(token);
  }
  return tokens;
}

/**
 * Parses `runtime.argv`, routes to the matching command (or to help/version), runs it, and
 * resolves to an exit code. The only place that decides *which* command runs and how its output
 * is formatted; a command itself never touches the registry, the raw argv, or a global flag.
 */
export async function dispatch(runtime: Runtime): Promise<number> {
  const registry = createRegistry(ALL_COMMANDS);
  const groups = commandGroups(registry);
  const { global, rest: withoutGlobalFlags } = extractGlobalFlags(runtime.argv);

  if (global.version || withoutGlobalFlags[0] === 'version') {
    return runVersionCommand(runtime);
  }

  const format = resolveOutputFormat({ jsonFlag: global.jsonFlag, formatFlag: global.formatFlag, env: runtime.env });
  const useColor = shouldUseColor({
    isTty: runtime.isTty,
    env: runtime.env,
    colorFlag: global.colorFlag,
    noColorFlag: global.noColorFlag,
  });
  const logLevel = verbosityToLogLevel(global.quiet, global.verbosity);
  const output = createCommandOutput({ stdout: runtime.stdout, stderr: runtime.stderr, format, useColor, logLevel });

  const helpRequested = withoutGlobalFlags[0] === 'help';
  const searchArgv = helpRequested ? withoutGlobalFlags.slice(1) : withoutGlobalFlags;
  const { path, rest } = resolveCommandPath(registry, searchArgv);
  const candidatePath = path.length > 0 ? path : attemptedPath(searchArgv);
  const wantsHelp = helpRequested || global.help;
  const isKnownPath =
    candidatePath.length === 0 || registry.has(candidatePath.join(' ')) || groups.has(candidatePath.join(' '));

  if (wantsHelp || !isKnownPath) {
    return runHelpCommand(runtime, ALL_COMMANDS, groups, candidatePath, HELP_OPTIONS);
  }

  const command = registry.get(path.join(' '));
  if (command === undefined) {
    return runHelpCommand(runtime, ALL_COMMANDS, groups, candidatePath, HELP_OPTIONS);
  }

  try {
    const parsed = parseCommandArgs(rest, command.flags ?? [], command.positionals ?? []);
    return await command.run({ runtime, flags: parsed.flags, positionals: parsed.positionals, output });
  } catch (error) {
    if (error instanceof CliUsageError) {
      output.error(error.message);
      return EXIT_CODES.usage;
    }
    throw error;
  }
}
