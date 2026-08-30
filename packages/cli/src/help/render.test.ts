import { describe, expect, it } from 'vitest';
import type { CommandSpec } from '../args/define';
import { commandGroups, createRegistry } from '../registry';
import { renderGroupHelp, renderLeafHelp, renderRootHelp } from './render';

const OPTIONS = { programName: 'kafka' };

const FIXTURE_COMMANDS: CommandSpec[] = [
  {
    path: ['ping'],
    summary: 'Check connectivity to the cluster',
    exitCodes: [0, 1],
    run: async () => 0,
  },
  {
    path: ['topic', 'list'],
    summary: 'List every topic',
    exitCodes: [0, 1],
    run: async () => 0,
  },
  {
    path: ['topic', 'create'],
    summary: 'Create one or more topics',
    flags: [
      { name: 'partitions', type: 'number', alias: 'p', brief: 'number of partitions' },
      { name: 'dry-run', type: 'boolean', brief: 'validate without creating' },
      { name: 'config', type: 'string', multiple: true, keyValue: true, brief: 'a topic config entry, key=value' },
    ],
    positionals: [{ name: 'topics', variadic: true, brief: 'topic names to create' }],
    examples: ['topic create orders --partitions 3', 'topic create orders payments --dry-run'],
    exitCodes: [0, 1, 4],
    run: async () => 0,
  },
];

describe('renderRootHelp', () => {
  it('matches the golden snapshot', () => {
    const registry = createRegistry(FIXTURE_COMMANDS);
    const groups = commandGroups(registry);
    expect(renderRootHelp(FIXTURE_COMMANDS, groups, OPTIONS)).toMatchInlineSnapshot(`
      "kafka — command-line admin client for Apache Kafka

      Usage: kafka <command> [flags]

      Commands:
        ping
        topic

      Global flags:
        --json                 emit exactly one JSON document on stdout
        --format <human|json>  same as --json when set to json
        -q, --quiet            only errors on stderr
        -v, --verbose          more detail on stderr (repeatable: -vv)
        --no-color             disable colored output
        --help                 show help for the given command
        --version              print the CLI version

      Run "kafka help <command>" for details on one command."
    `);
  });
});

describe('renderGroupHelp', () => {
  it('matches the golden snapshot', () => {
    expect(renderGroupHelp(['topic'], FIXTURE_COMMANDS, OPTIONS)).toMatchInlineSnapshot(`
      "Usage: kafka topic <subcommand> [flags]

      Subcommands:
        topic list  List every topic
        topic create  Create one or more topics

      Run "kafka help topic <subcommand>" for details."
    `);
  });
});

describe('renderLeafHelp', () => {
  it('matches the golden snapshot', () => {
    const command = FIXTURE_COMMANDS[2];
    if (command === undefined) throw new Error('fixture command missing');
    expect(renderLeafHelp(command, OPTIONS)).toMatchInlineSnapshot(`
      "Usage: kafka topic create <topics...> [flags]

      Create one or more topics

      Flags:
        -p, --partitions <number>  number of partitions
        --dry-run  validate without creating
        --config <key=value> (repeatable)  a topic config entry, key=value

      Examples:
        kafka topic create orders --partitions 3
        kafka topic create orders payments --dry-run"
    `);
  });

  it('marks an unstable command in the usage line and summary', () => {
    const command: CommandSpec = {
      path: ['admin', 'call'],
      summary: 'Call any Admin method by name',
      exitCodes: [0],
      unstable: true,
      run: async () => 0,
    };
    const rendered = renderLeafHelp(command, OPTIONS);
    expect(rendered).toContain('[unstable]');
    expect(rendered).toContain('unstable: its argument and result shape');
  });
});
