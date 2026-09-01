import { describe, expect, it } from 'vitest';
import { CliUsageError } from './coerce';
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

  it('throws CliUsageError for an unrecognized --format value instead of silently defaulting', () => {
    expect(() => extractGlobalFlags(['--format', 'yaml'])).toThrow(CliUsageError);
    expect(() => extractGlobalFlags(['--format', 'yaml'])).toThrow(/--format expects "human" or "json"/);
  });

  it('throws CliUsageError when --format is the last token, rather than swallowing nothing', () => {
    expect(() => extractGlobalFlags(['topic', 'list', '--format'])).toThrow(CliUsageError);
  });

  it("does not swallow the next flag as --format's value when it is not a valid format", () => {
    // Regression: --format used to consume *any* next token unconditionally, so a real flag
    // right after it (e.g. -v) would be silently discarded instead of being parsed.
    expect(() => extractGlobalFlags(['--format', '-v', '--brokers', 'x'])).toThrow(CliUsageError);
  });

  it('extracts --config-file <path> and --config-file=<path>', () => {
    expect(extractGlobalFlags(['--config-file', './kafka.config.ts']).global.configFlag).toBe('./kafka.config.ts');
    expect(extractGlobalFlags(['--config-file=./kafka.config.ts']).global.configFlag).toBe('./kafka.config.ts');
  });

  it('throws CliUsageError when --config-file is the last token', () => {
    expect(() => extractGlobalFlags(['ping', '--config-file'])).toThrow(CliUsageError);
  });

  it("does not swallow a following flag as --config-file's value", () => {
    // Regression: --config-file used to take argv[i+1] unconditionally, so
    // "--config-file --profile staging" silently consumed "--profile" as the path and left
    // "staging" behind as a stray token instead of reporting a usage error.
    expect(() => extractGlobalFlags(['--config-file', '--profile', 'staging'])).toThrow(CliUsageError);
  });

  it('extracts --profile <name> and --profile=<name>', () => {
    expect(extractGlobalFlags(['--profile', 'staging']).global.profileFlag).toBe('staging');
    expect(extractGlobalFlags(['--profile=staging']).global.profileFlag).toBe('staging');
  });

  it("does not swallow a following flag as --profile's value", () => {
    expect(() => extractGlobalFlags(['--profile', '--config-file', 'x.ts'])).toThrow(CliUsageError);
  });

  it('throws CliUsageError when --profile is the last token', () => {
    expect(() => extractGlobalFlags(['ping', '--profile'])).toThrow(CliUsageError);
  });

  it("never confuses --config-file with topic create's own --config key=value flag", () => {
    // Regression risk: a global flag reserved under the wrong name would swallow a command's own
    // same-named flag before that command's parser ever saw it (see registry.ts's reserved names).
    const { global, rest } = extractGlobalFlags(['topic', 'create', 'orders', '--config', 'retention.ms=1000']);
    expect(global.configFlag).toBeUndefined();
    expect(rest).toEqual(['topic', 'create', 'orders', '--config', 'retention.ms=1000']);
  });
});
