import { describe, expect, it } from 'vitest';
import { CliUsageError } from './coerce';
import type { FlagSpec, PositionalSpec } from './define';
import { parseCommandArgs } from './parse';

describe('parseCommandArgs', () => {
  it('parses a string flag', () => {
    const flags: FlagSpec[] = [{ name: 'name', type: 'string', brief: '' }];
    expect(parseCommandArgs(['--name', 'orders'], flags).flags).toEqual({ name: 'orders' });
  });

  it('parses a boolean flag present as true', () => {
    const flags: FlagSpec[] = [{ name: 'wait', type: 'boolean', brief: '' }];
    expect(parseCommandArgs(['--wait'], flags).flags).toEqual({ wait: true });
  });

  it('leaves an absent boolean flag out of the result', () => {
    const flags: FlagSpec[] = [{ name: 'wait', type: 'boolean', brief: '' }];
    expect(parseCommandArgs([], flags).flags).toEqual({});
  });

  it('parses a number flag', () => {
    const flags: FlagSpec[] = [{ name: 'partitions', type: 'number', brief: '' }];
    expect(parseCommandArgs(['--partitions', '3'], flags).flags).toEqual({ partitions: 3 });
  });

  it('throws CliUsageError for a non-numeric number flag', () => {
    const flags: FlagSpec[] = [{ name: 'partitions', type: 'number', brief: '' }];
    expect(() => parseCommandArgs(['--partitions', 'three'], flags)).toThrow(CliUsageError);
  });

  it('parses an enum flag', () => {
    const flags: FlagSpec[] = [{ name: 'time', type: 'enum', values: ['earliest', 'latest'], brief: '' }];
    expect(parseCommandArgs(['--time', 'earliest'], flags).flags).toEqual({ time: 'earliest' });
  });

  it('throws CliUsageError for an out-of-set enum value', () => {
    const flags: FlagSpec[] = [{ name: 'time', type: 'enum', values: ['earliest', 'latest'], brief: '' }];
    expect(() => parseCommandArgs(['--time', 'yesterday'], flags)).toThrow(CliUsageError);
  });

  it('parses a repeated --config k=v flag into a record', () => {
    const flags: FlagSpec[] = [{ name: 'config', type: 'string', multiple: true, keyValue: true, brief: '' }];
    const result = parseCommandArgs(['--config', 'a=1', '--config', 'b=2'], flags);
    expect(result.flags).toEqual({ config: { a: '1', b: '2' } });
  });

  it('parses a repeated non-key-value flag into an array', () => {
    const flags: FlagSpec[] = [{ name: 'tag', type: 'string', multiple: true, brief: '' }];
    expect(parseCommandArgs(['--tag', 'a', '--tag', 'b'], flags).flags).toEqual({ tag: ['a', 'b'] });
  });

  it('honors an alias', () => {
    const flags: FlagSpec[] = [{ name: 'partitions', type: 'number', alias: 'p', brief: '' }];
    expect(parseCommandArgs(['-p', '3'], flags).flags).toEqual({ partitions: 3 });
  });

  it('applies --no-<flag> to a negatable boolean', () => {
    const flags: FlagSpec[] = [{ name: 'wait', type: 'boolean', negatable: true, brief: '' }];
    expect(parseCommandArgs(['--no-wait'], flags).flags).toEqual({ wait: false });
  });

  it('does not treat --no-<flag> specially for a non-negatable boolean', () => {
    const flags: FlagSpec[] = [{ name: 'wait', type: 'boolean', brief: '' }];
    expect(() => parseCommandArgs(['--no-wait'], flags)).toThrow(CliUsageError);
  });

  it('passes everything after -- through as positionals, untouched', () => {
    const flags: FlagSpec[] = [{ name: 'wait', type: 'boolean', brief: '' }];
    const result = parseCommandArgs(['--', '--wait', 'literal'], flags);
    expect(result.positionals).toEqual(['--wait', 'literal']);
    expect(result.flags).toEqual({});
  });

  it('throws CliUsageError for an unknown flag', () => {
    const flags: FlagSpec[] = [{ name: 'wait', type: 'boolean', brief: '' }];
    expect(() => parseCommandArgs(['--nope'], flags)).toThrow(CliUsageError);
  });

  it('collects ordinary positionals', () => {
    const result = parseCommandArgs(['orders', 'payments'], []);
    expect(result.positionals).toEqual(['orders', 'payments']);
  });

  it('requires a declared positional', () => {
    const positionals: PositionalSpec[] = [{ name: 'topic', brief: '' }];
    expect(() => parseCommandArgs([], [], positionals)).toThrow(CliUsageError);
  });

  it('rejects an unexpected extra positional when none are variadic', () => {
    const positionals: PositionalSpec[] = [{ name: 'topic', brief: '' }];
    expect(() => parseCommandArgs(['orders', 'extra'], [], positionals)).toThrow(CliUsageError);
  });

  it('accepts any number of positionals when the last one is variadic', () => {
    const positionals: PositionalSpec[] = [{ name: 'topics', variadic: true, brief: '' }];
    const result = parseCommandArgs(['orders', 'payments', 'refunds'], [], positionals);
    expect(result.positionals).toEqual(['orders', 'payments', 'refunds']);
  });
});
