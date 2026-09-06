import { describe, expect, it } from 'vitest';
import type { MessageRecord } from '../../../shared/contracts/message';
import { toJsonl } from './export';

function record(overrides: Partial<MessageRecord> = {}): MessageRecord {
  return {
    partition: 0,
    offset: '0',
    timestamp: '1700000000000',
    key: null,
    value: null,
    headers: {},
    size: 0,
    ...overrides,
  };
}

function toBase64(text: string): string {
  return btoa(text);
}

describe('toJsonl', () => {
  it('renders one JSON line per message, decoding key/value/headers', () => {
    const message = record({
      key: toBase64('k1'),
      value: toBase64('{"a":1}'),
      headers: { 'content-type': toBase64('json') },
    });

    const lines = toJsonl([message], 'utf8').split('\n');
    expect(lines).toHaveLength(1);
    expect(JSON.parse(lines[0] ?? '{}')).toEqual({
      partition: 0,
      offset: '0',
      timestamp: '1700000000000',
      key: 'k1',
      value: '{"a":1}',
      headers: { 'content-type': 'json' },
    });
  });

  it('keeps a null key/value as null rather than decoding it', () => {
    const lines = toJsonl([record()], 'utf8').split('\n');
    expect(JSON.parse(lines[0] ?? '{}')).toMatchObject({ key: null, value: null });
  });

  it('joins multiple messages with one line each', () => {
    const lines = toJsonl([record({ offset: '1' }), record({ offset: '2' })], 'utf8').split('\n');
    expect(lines).toHaveLength(2);
  });

  it('returns an empty string for no messages', () => {
    expect(toJsonl([], 'utf8')).toBe('');
  });
});
