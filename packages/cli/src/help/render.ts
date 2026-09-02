import type { CommandSpec, FlagSpec } from '../args/define';

export interface HelpRenderOptions {
  readonly programName: string;
}

const GLOBAL_FLAGS_HELP = [
  '  --json                 emit exactly one JSON document on stdout',
  '  --format <human|json>  same as --json when set to json',
  '  --config-file <path>   use this kafka.config.* file instead of discovering one',
  '  --profile <name>       select a named connection profile from cli.profiles',
  '  -q, --quiet            only errors on stderr',
  '  -v, --verbose          more detail on stderr (repeatable: -vv)',
  '  --no-color             disable colored output',
  '  --help                 show help for the given command',
  '  --version              print the CLI version',
].join('\n');

function flagUsage(flag: FlagSpec): string {
  const alias = flag.alias !== undefined ? `-${flag.alias}, ` : '';
  if (flag.type === 'boolean') return `${alias}--${flag.name}`;
  if (flag.keyValue === true)
    return `${alias}--${flag.name} <key=value>${flag.multiple === true ? ' (repeatable)' : ''}`;
  if (flag.type === 'enum') return `${alias}--${flag.name} <${(flag.values ?? []).join('|')}>`;
  return `${alias}--${flag.name} <${flag.type}>${flag.multiple === true ? ' (repeatable)' : ''}`;
}

function renderCommandLine(command: CommandSpec): string {
  const positionals = (command.positionals ?? [])
    .map((p) => (p.variadic === true ? `<${p.name}...>` : `<${p.name}>`))
    .join(' ');
  return [command.path.join(' '), positionals].filter((part) => part.length > 0).join(' ');
}

export function renderRootHelp(
  leaves: readonly CommandSpec[],
  groups: ReadonlySet<string>,
  options: HelpRenderOptions,
): string {
  const topLevel = new Set<string>();
  for (const command of leaves) {
    if (command.hidden === true) continue;
    const [first] = command.path;
    if (first !== undefined) topLevel.add(groups.has(first) ? first : command.path.join(' '));
  }

  const lines = [
    `${options.programName} — command-line admin client for Apache Kafka`,
    '',
    `Usage: ${options.programName} <command> [flags]`,
    '',
    'Commands:',
    ...[...topLevel].sort().map((name) => `  ${name}`),
    '',
    'Global flags:',
    GLOBAL_FLAGS_HELP,
    '',
    `Run "${options.programName} help <command>" for details on one command.`,
  ];
  return lines.join('\n');
}

export function renderGroupHelp(
  groupPath: readonly string[],
  leaves: readonly CommandSpec[],
  options: HelpRenderOptions,
): string {
  const prefix = groupPath.join(' ');
  const children = leaves.filter(
    (command) => command.hidden !== true && command.path.slice(0, groupPath.length).join(' ') === prefix,
  );

  const lines = [
    `Usage: ${options.programName} ${prefix} <subcommand> [flags]`,
    '',
    'Subcommands:',
    ...children.map(
      (command) => `  ${command.path.join(' ')}  ${command.summary}${command.unstable === true ? ' (unstable)' : ''}`,
    ),
    '',
    `Run "${options.programName} help ${prefix} <subcommand>" for details.`,
  ];
  return lines.join('\n');
}

export function renderLeafHelp(command: CommandSpec, options: HelpRenderOptions): string {
  const unstableSuffix = command.unstable === true ? ' [unstable]' : '';
  const lines = [
    `Usage: ${options.programName} ${renderCommandLine(command)} [flags]${unstableSuffix}`,
    '',
    command.unstable === true
      ? `${command.summary} — unstable: its argument and result shape tracks core's own types and is not frozen for 1.x.`
      : command.summary,
  ];

  if (command.flags !== undefined && command.flags.length > 0) {
    lines.push('', 'Flags:');
    for (const flag of command.flags) {
      lines.push(`  ${flagUsage(flag)}  ${flag.brief}`);
    }
  }

  if (command.examples !== undefined && command.examples.length > 0) {
    lines.push('', 'Examples:');
    for (const example of command.examples) {
      lines.push(`  ${options.programName} ${example}`);
    }
  }

  return lines.join('\n');
}
