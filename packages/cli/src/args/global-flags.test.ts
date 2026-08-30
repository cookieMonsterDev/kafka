import { describe, expect, it } from 'vitest';
import { extractGlobalFlags } from './pre-parse';

describe('extractGlobalFlags', () => {
  it('extracts nothing from ordinary command tokens', () => {
    const { global, rest } = extractGlobalFlags(['topic', 'list']);
    expect(rest).toEqual(['topic', 'list']);
    expect(global).toEqual({
      jsonFlag: false,
      formatFlag: undefined,
      quiet: false,
      verbosity: 0,
      colorFlag: false,
      noColorFlag: false,
      help: false,
      version: false,
    });
  });

  it('extracts --json', () => {
    expect(extractGlobalFlags(['topic', 'list', '--json']).global.jsonFlag).toBe(true);
  });

  it('extracts --format json / --format=json / --format human', () => {
    expect(extractGlobalFlags(['topic', 'list', '--format', 'json']).global.formatFlag).toBe('json');
    expect(extractGlobalFlags(['topic', 'list', '--format=json']).global.formatFlag).toBe('json');
    expect(extractGlobalFlags(['topic', 'list', '--format', 'human']).global.formatFlag).toBe('human');
  });

  it('counts repeated -v/--verbose', () => {
    expect(extractGlobalFlags(['-v']).global.verbosity).toBe(1);
    expect(extractGlobalFlags(['-v', '--verbose']).global.verbosity).toBe(2);
  });

  it('extracts -q/--quiet', () => {
    expect(extractGlobalFlags(['-q']).global.quiet).toBe(true);
    expect(extractGlobalFlags(['--quiet']).global.quiet).toBe(true);
  });

  it('extracts --color and --no-color', () => {
    expect(extractGlobalFlags(['--color']).global.colorFlag).toBe(true);
    expect(extractGlobalFlags(['--no-color']).global.noColorFlag).toBe(true);
  });

  it('extracts -h/--help and --version', () => {
    expect(extractGlobalFlags(['-h']).global.help).toBe(true);
    expect(extractGlobalFlags(['--help']).global.help).toBe(true);
    expect(extractGlobalFlags(['--version']).global.version).toBe(true);
  });

  it('removes every extracted flag from rest, leaving command-specific tokens', () => {
    const { rest } = extractGlobalFlags(['topic', 'create', 'orders', '--json', '-v', '--partitions', '3']);
    expect(rest).toEqual(['topic', 'create', 'orders', '--partitions', '3']);
  });

  it('does not extract anything after a -- terminator', () => {
    const { global, rest } = extractGlobalFlags(['topic', 'create', '--', '--json', '-v']);
    expect(global.jsonFlag).toBe(false);
    expect(global.verbosity).toBe(0);
    expect(rest).toEqual(['topic', 'create', '--', '--json', '-v']);
  });
});
