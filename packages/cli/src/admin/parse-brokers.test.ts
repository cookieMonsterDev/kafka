import { describe, expect, it } from 'vitest';
import { CliUsageError } from '../args/coerce';
import { parseBrokersFlag } from './parse-brokers';

describe('parseBrokersFlag', () => {
  it('splits a comma-separated list and trims whitespace', () => {
    expect(parseBrokersFlag(' a:1 , b:2,c:3 ')).toEqual(['a:1', 'b:2', 'c:3']);
  });

  it('accepts a single broker', () => {
    expect(parseBrokersFlag('localhost:9092')).toEqual(['localhost:9092']);
  });

  it('drops empty entries from stray commas', () => {
    expect(parseBrokersFlag('a:1,,b:2,')).toEqual(['a:1', 'b:2']);
  });

  it('returns undefined when the flag is missing, letting a lower layer resolve brokers', () => {
    expect(parseBrokersFlag(undefined)).toBeUndefined();
  });

  it('throws CliUsageError when the flag resolves to nothing usable', () => {
    expect(() => parseBrokersFlag(' , , ')).toThrow(CliUsageError);
  });

  it('throws CliUsageError for a non-string value', () => {
    expect(() => parseBrokersFlag(42)).toThrow(CliUsageError);
  });
});
