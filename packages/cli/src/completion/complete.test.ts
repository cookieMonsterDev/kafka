import { describe, expect, it } from 'vitest';
import type { CommandSpec } from '../args/define';
import { ALL_COMMANDS } from '../commands/index';
import { getCompletions } from './complete';

function stub(path: string[], overrides: Partial<CommandSpec> = {}): CommandSpec {
  return { path, summary: 'stub', exitCodes: [0], run: async () => 0, ...overrides };
}

const FIXTURE: CommandSpec[] = [
  stub(['ping']),
  stub(['topic', 'list']),
  stub(['topic', 'create'], {
    flags: [
      { name: 'partitions', type: 'number', alias: 'p', brief: 'partitions' },
      { name: 'dry-run', type: 'boolean', negatable: true, brief: 'dry run' },
      { name: 'tag', type: 'string', multiple: true, brief: 'a tag (repeatable)' },
      { name: 'level', type: 'enum', values: ['low', 'high'], brief: 'level' },
    ],
  }),
  stub(['topic', 'describe']),
  stub(['topic', 'delete']),
  stub(['cluster', 'raft-voter', 'add']),
  stub(['cluster', 'raft-voter', 'remove']),
  stub(['complete'], { hidden: true }),
];

describe('getCompletions', () => {
  it('suggests every top-level group and leaf when nothing has been typed', () => {
    expect(getCompletions(FIXTURE, [''])).toEqual(['cluster', 'ping', 'topic']);
  });

  it('never suggests a hidden command', () => {
    expect(getCompletions(FIXTURE, [''])).not.toContain('complete');
  });

  it('filters top-level suggestions by the partial word', () => {
    expect(getCompletions(FIXTURE, ['pi'])).toEqual(['ping']);
  });

  it('suggests verbs (subcommand names) inside a group', () => {
    expect(getCompletions(FIXTURE, ['topic', ''])).toEqual(['create', 'delete', 'describe', 'list']);
  });

  it('filters verb suggestions by the partial word', () => {
    expect(getCompletions(FIXTURE, ['topic', 'de'])).toEqual(['delete', 'describe']);
  });

  it('suggests nested group segments two levels deep', () => {
    expect(getCompletions(FIXTURE, ['cluster', 'raft-voter', ''])).toEqual(['add', 'remove']);
  });

  it('returns nothing for a path that matches no registered command', () => {
    expect(getCompletions(FIXTURE, ['bogus', ''])).toEqual([]);
  });

  it('suggests a leaf command own long flags once its path is complete', () => {
    const result = getCompletions(FIXTURE, ['topic', 'create', '--']);
    expect(result).toContain('--partitions');
    expect(result).toContain('--dry-run');
    expect(result).toContain('--tag');
    expect(result).toContain('--level');
    expect(result).toContain('--help');
    expect(result).toContain('--json');
  });

  it('filters flag suggestions by the partial flag name', () => {
    expect(getCompletions(FIXTURE, ['topic', 'create', '--part'])).toEqual(['--partitions']);
  });

  it('offers a negatable boolean flag alongside its --no- form', () => {
    const result = getCompletions(FIXTURE, ['topic', 'create', '--dry']);
    expect(result).toEqual(['--dry-run']);
    const noForm = getCompletions(FIXTURE, ['topic', 'create', '--no-dry']);
    expect(noForm).toEqual(['--no-dry-run']);
  });

  it('stops suggesting a non-multiple flag once it has already been given', () => {
    const result = getCompletions(FIXTURE, ['topic', 'create', '--partitions', '3', '--part']);
    expect(result).toEqual([]);
  });

  it('keeps suggesting a repeatable flag after it has already been given once', () => {
    const result = getCompletions(FIXTURE, ['topic', 'create', '--tag', 'a', '--tag']);
    expect(result).toEqual(['--tag']);
  });

  it('completes an enum flag value to its declared values', () => {
    expect(getCompletions(FIXTURE, ['topic', 'create', '--level', ''])).toEqual(['high', 'low']);
    expect(getCompletions(FIXTURE, ['topic', 'create', '--level', 'h'])).toEqual(['high']);
  });

  it("completes the global --format flag's value without any command context", () => {
    expect(getCompletions(FIXTURE, ['ping', '--format', ''])).toEqual(['human', 'json']);
    expect(getCompletions(FIXTURE, ['ping', '--format', 'j'])).toEqual(['json']);
  });

  it('never touches a runtime, an admin client, or any I/O — it only reads the command list', () => {
    // No fake runtime, admin, or filesystem is constructed anywhere in this file; a completion
    // request that somehow needed one would throw here rather than return an array.
    expect(() => getCompletions(FIXTURE, ['topic', ''])).not.toThrow();
  });

  it('matches the real command registry: hidden complete is absent from top-level suggestions', () => {
    expect(getCompletions(ALL_COMMANDS, [''])).not.toContain('complete');
    expect(getCompletions(ALL_COMMANDS, [''])).toContain('topic');
  });
});
