import { describe, expect, it } from 'vitest';
import { decodeMessageField } from './decode';

function toBase64(text: string): string {
  return btoa(text);
}

describe('decodeMessageField', () => {
  it('renders null as the literal text "null", regardless of decoder', () => {
    expect(decodeMessageField(null, 'utf8')).toEqual({ text: 'null' });
    expect(decodeMessageField(null, 'json')).toEqual({ text: 'null' });
    expect(decodeMessageField(null, 'hex')).toEqual({ text: 'null' });
    expect(decodeMessageField(null, 'base64')).toEqual({ text: 'null' });
  });

  it('decodes utf8', () => {
    expect(decodeMessageField(toBase64('hello'), 'utf8')).toEqual({ text: 'hello' });
  });

  it('pretty-prints valid json', () => {
    const result = decodeMessageField(toBase64('{"a":1}'), 'json');
    expect(result.error).toBeUndefined();
    expect(result.text).toBe(JSON.stringify({ a: 1 }, null, 2));
  });

  it('falls back to the raw text with an error when json is invalid', () => {
    const result = decodeMessageField(toBase64('not json'), 'json');
    expect(result.error).toBe('not valid JSON');
    expect(result.text).toBe('not json');
  });

  it('renders hex', () => {
    expect(decodeMessageField(toBase64('AB'), 'hex')).toEqual({ text: '4142' });
  });

  it('returns the base64 unchanged for the base64 decoder', () => {
    const encoded = toBase64('hello');
    expect(decodeMessageField(encoded, 'base64')).toEqual({ text: encoded });
  });

  it('reports invalid base64 rather than throwing', () => {
    const result = decodeMessageField('not-base64!!', 'utf8');
    expect(result.error).toBe('not valid base64');
  });
});
