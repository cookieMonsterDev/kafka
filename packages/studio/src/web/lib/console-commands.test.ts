import { describe, expect, it } from 'vitest';
import { acceptConsoleSuggestion, parseConsoleCommand, suggestConsoleCommands } from './console-commands';

const resources = {
  topics: ['orders', 'other'],
  groups: ['payments worker', 'quoted"group', 'team\\reader', 'line\nbreak', 'tab\tgroup', '--reader', '--help'],
};
const suggest = (line: string, cursor = line.length) => suggestConsoleCommands(line, cursor, resources);

describe('console command parsing', () => {
  it('accepts the CLI prefix and config flags before or after the resource', () => {
    expect(parseConsoleCommand('kafka topic describe orders').resource).toBe('orders');
    expect(parseConsoleCommand('config describe --type topic orders').resource).toBe('orders');
    expect(parseConsoleCommand('config describe orders --type topic').resource).toBe('orders');
  });

  it('handles quoted group IDs, escaped spaces and backslashes', () => {
    expect(parseConsoleCommand('group describe "payments worker"').resource).toBe('payments worker');
    expect(parseConsoleCommand("group describe 'payments worker'").resource).toBe('payments worker');
    expect(parseConsoleCommand('group describe payments\\ worker').resource).toBe('payments worker');
    expect(parseConsoleCommand('group describe "team\\\\reader"').resource).toBe('team\\reader');
  });

  it('treats names after the end-of-options marker literally', () => {
    expect(parseConsoleCommand('group describe -- --help').resource).toBe('--help');
    expect(parseConsoleCommand('group describe -- --help').help).toBe(false);
    expect(parseConsoleCommand('config describe --type topic -- --topic').resource).toBe('--topic');
  });

  it('shows command help without requiring arguments or a connection', () => {
    expect(parseConsoleCommand('topic describe --help').help).toBe(true);
    expect(parseConsoleCommand('config describe --help').help).toBe(true);
  });

  it.each([
    '',
    'topic delete orders',
    'admin call deleteTopics',
    'topic list --brokers other:9092',
    'topic describe',
    'topic describe ""',
    'topic describe orders extra',
    'topic list ; topic delete orders',
    'topic list | cat',
    'group describe "unfinished',
    'group describe trailing\\',
    'config describe --type broker orders',
    'config describe orders',
    'group list --profile prod',
  ])('rejects invalid or unsupported input: %s', (line) => {
    expect(() => parseConsoleCommand(line)).toThrow();
  });
});

describe('console suggestions', () => {
  it('completes commands, subcommands, flags and allowed values', () => {
    expect(suggest('top').map((item) => item.label)).toEqual(['topic']);
    expect(suggest('kafka topic d').map((item) => item.label)).toEqual(['describe']);
    expect(suggest('config describe --t').map((item) => item.label)).toEqual(['--type']);
    expect(suggest('config describe --type ').map((item) => item.label)).toEqual(['topic']);
    expect(suggest('topic describe --h').map((item) => item.label)).toEqual(['--help']);
  });

  it('suggests live resource names in positional arguments', () => {
    expect(suggest('topic describe or').map((item) => item.label)).toEqual(['orders']);
    expect(suggest('config describe --type topic or').map((item) => item.label)).toEqual(['orders']);
    expect(suggest('group offsets pay').map((item) => item.label)).toEqual(['payments worker']);
    expect(suggest('topic describe orders ').map((item) => item.label)).not.toContain('other');
  });

  it('quotes inserted group names so execution round-trips special characters', () => {
    for (const name of resources.groups) {
      const suggestion = suggest('group describe ').find((item) => item.label === name);
      expect(suggestion).toBeDefined();
      if (suggestion === undefined) continue;
      const replacement = acceptConsoleSuggestion('group describe ', suggestion);
      expect(parseConsoleCommand(replacement.line).resource).toBe(name);
    }
  });

  it('completes incomplete quotes and preserves arguments after the caret', () => {
    const line = 'config describe ord --type topic';
    const suggestion = suggest(line, 'config describe or'.length)[0];
    expect(suggestion?.label).toBe('orders');
    if (suggestion === undefined) throw new Error('Expected an orders suggestion');
    const accepted = acceptConsoleSuggestion(line, suggestion);
    expect(accepted.line).toBe('config describe orders --type topic');
    expect(accepted.cursor).toBe('config describe orders'.length);
    expect(suggest('group describe "payments ').map((item) => item.label)).toContain('payments worker');
  });

  it('bounds large resource lists', () => {
    const suggestions = suggestConsoleCommands('topic describe ', 15, {
      topics: Array.from({ length: 100 }, (_, index) => `topic-${String(index)}`),
      groups: [],
    });
    expect(suggestions.length).toBeLessThanOrEqual(30);
  });
});
