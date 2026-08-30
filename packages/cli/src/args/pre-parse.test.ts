import { describe, expect, it } from 'vitest';
import { preParseOutputFormat } from './pre-parse';

describe('preParseOutputFormat', () => {
  it('defaults to human when neither flag is present', () => {
    expect(preParseOutputFormat(['topic', 'list'])).toBe('human');
  });

  it('finds --json anywhere before a -- terminator', () => {
    expect(preParseOutputFormat(['topic', 'list', '--json'])).toBe('json');
    expect(preParseOutputFormat(['--json', 'topic', 'list'])).toBe('json');
  });

  it('finds --format json and --format=json', () => {
    expect(preParseOutputFormat(['topic', 'list', '--format', 'json'])).toBe('json');
    expect(preParseOutputFormat(['topic', 'list', '--format=json'])).toBe('json');
  });

  it('ignores --format human', () => {
    expect(preParseOutputFormat(['topic', 'list', '--format', 'human'])).toBe('human');
  });

  it('finds --json even when a real parse error would occur later', () => {
    expect(preParseOutputFormat(['topic', 'list', '--unknown-flag', '--json'])).toBe('json');
  });

  it('ignores --json after a -- terminator', () => {
    expect(preParseOutputFormat(['topic', 'create', '--', '--json'])).toBe('human');
  });
});
