import { describe, expect, it, vi } from 'vitest';
import { readCliSection } from './cli-section';

describe('readCliSection', () => {
  it('returns an empty section when the file has no cli: key', () => {
    expect(readCliSection({ client: {} })).toEqual({});
    expect(readCliSection(null)).toEqual({});
  });

  it('reads every known key', () => {
    const section = readCliSection({
      cli: {
        output: 'json',
        confirmDestructive: false,
        timeoutMs: 5000,
        topicDefaults: { partitions: 3, replicationFactor: 2 },
        profiles: { staging: { brokers: ['s:1'] } },
      },
    });

    expect(section).toEqual({
      output: 'json',
      confirmDestructive: false,
      timeoutMs: 5000,
      topicDefaults: { partitions: 3, replicationFactor: 2 },
      profiles: { staging: { brokers: ['s:1'] } },
    });
  });

  it('warns and ignores an unknown top-level cli key, never throwing', () => {
    const warn = vi.fn();
    const section = readCliSection({ cli: { output: 'json', bogus: true } }, warn);

    expect(section.output).toBe('json');
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('cli.bogus'));
  });

  it('warns and ignores cli when it is not an object', () => {
    const warn = vi.fn();
    expect(readCliSection({ cli: 'nope' }, warn)).toEqual({});
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('"cli" section'));
  });

  it('warns and drops topicDefaults when it is not an object', () => {
    const warn = vi.fn();
    const section = readCliSection({ cli: { topicDefaults: 'nope' } }, warn);
    expect(section.topicDefaults).toBeUndefined();
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('cli.topicDefaults'));
  });

  it('warns and drops profiles when it is not an object', () => {
    const warn = vi.fn();
    const section = readCliSection({ cli: { profiles: 'nope' } }, warn);
    expect(section.profiles).toBeUndefined();
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('cli.profiles'));
  });

  it('drops an individual profile entry that is not an object, keeping the rest', () => {
    const warn = vi.fn();
    const section = readCliSection({ cli: { profiles: { staging: { brokers: ['s:1'] }, bogus: 'nope' } } }, warn);
    expect(section.profiles).toEqual({ staging: { brokers: ['s:1'] } });
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('cli.profiles.bogus'));
  });

  it('never throws for any input', () => {
    expect(() => readCliSection({ cli: null })).not.toThrow();
    expect(() => readCliSection({ cli: [] })).not.toThrow();
  });
});
