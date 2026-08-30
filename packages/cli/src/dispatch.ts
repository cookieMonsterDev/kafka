import type { CommandSpec } from './args/define';
import { extractGlobalFlags } from './args/pre-parse';
import { parseCommandArgs } from './args/parse';
import { ALL_COMMANDS } from './commands/index';
import { runHelpCommand } from './commands/meta/help';
import { runVersionCommand } from './commands/meta/version';
import { mapKafkaError } from './errors/map-kafka-error';
import { shouldUseColor } from './output/colors';
import { createCommandOutput, resolveOutputFormat } from './output/format';
import { verbosityToLogLevel } from './output/logger';
import type { HelpRenderOptions } from './help/render';
import { commandGroups, createRegistry } from './registry';
import type { Runtime } from './runtime';

const HELP_OPTIONS: HelpRenderOptions = { programName: 'kafka' };

// Built once, at module load — not per invocation inside dispatch() — so a bad mount (a
// duplicate path, a reserved flag name, a reused alias, an unknown exit code) throws the moment
// this module is imported, matching what the registry's own doc comment promises.
const REGISTRY = createRegistry(ALL_COMMANDS);
const GROUPS = commandGroups(REGISTRY);

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
 *
 * Everything from global-flag extraction onward is one try/catch: a malformed global flag (e.g.
 * `--format bogus`) throws before an output format is even known, same as any other usage error,
 * and must still resolve to an exit code rather than crash the process — it just falls back to
 * human/stderr for that one report, since the very flag that failed is what would have chosen
 * otherwise.
 */
export async function dispatch(runtime: Runtime): Promise<number> {
  try {
    const registry = REGISTRY;
    const groups = GROUPS;
    const { global, rest: withoutGlobalFlags } = extractGlobalFlags(runtime.argv);

    if (global.version || withoutGlobalFlags[0] === 'version') {
      return runVersionCommand(runtime);
    }

    const format = resolveOutputFormat({
      jsonFlag: global.jsonFlag,
      formatFlag: global.formatFlag,
      env: runtime.env,
    });
    const useColor = shouldUseColor({
      isTty: runtime.isTty,
      env: runtime.env,
      colorFlag: global.colorFlag,
      noColorFlag: global.noColorFlag,
    });
    const logLevel = verbosityToLogLevel(global.quiet, global.verbosity);
    const output = createCommandOutput({
      stdout: runtime.stdout,
      stderr: runtime.stderr,
      format,
      useColor,
      logLevel,
    });

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
      const mapped = mapKafkaError(error);
      output.cliError(mapped);
      return mapped.exitCode;
    }
  } catch (error) {
    const mapped = mapKafkaError(error);
    const fallbackOutput = createCommandOutput({
      stdout: runtime.stdout,
      stderr: runtime.stderr,
      format: 'human',
      useColor: false,
      logLevel: verbosityToLogLevel(false, 0),
    });
    fallbackOutput.cliError(mapped);
    return mapped.exitCode;
  }
}
