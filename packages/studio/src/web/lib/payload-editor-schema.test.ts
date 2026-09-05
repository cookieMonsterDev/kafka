import { describe, expect, it } from 'vitest';
import { buildProduceMessage, createEmptyPayloadValue, payloadEditorValueError } from './payload-editor-schema';

describe('payloadEditorValueError', () => {
  it('accepts an empty value', () => {
    expect(payloadEditorValueError(createEmptyPayloadValue())).toBeNull();
  });

  it('accepts valid JSON when the format is json', () => {
    const value = { ...createEmptyPayloadValue(), value: '{"a":1}' };
    expect(payloadEditorValueError(value)).toBeNull();
  });

  it('rejects invalid JSON when the format is json', () => {
    const value = { ...createEmptyPayloadValue(), value: '{not json' };
    expect(payloadEditorValueError(value)).toContain('not valid JSON');
  });

  it('accepts non-JSON text when the format is text', () => {
    const value = { ...createEmptyPayloadValue(), value: '{not json', valueFormat: 'text' as const };
    expect(payloadEditorValueError(value)).toBeNull();
  });

  it('rejects a non-numeric partition', () => {
    const value = { ...createEmptyPayloadValue(), partition: 'abc' };
    expect(payloadEditorValueError(value)).toContain('Partition');
  });

  it('accepts a numeric partition', () => {
    const value = { ...createEmptyPayloadValue(), partition: '3' };
    expect(payloadEditorValueError(value)).toBeNull();
  });
});

describe('buildProduceMessage', () => {
  it('omits key, partition, and headers when unset', () => {
    expect(buildProduceMessage(createEmptyPayloadValue())).toEqual({ value: '' });
  });

  it('includes a trimmed key when set', () => {
    const value = { ...createEmptyPayloadValue(), key: 'order-1' };
    expect(buildProduceMessage(value)).toEqual({ value: '', key: 'order-1' });
  });

  it('includes the partition as a number when set', () => {
    const value = { ...createEmptyPayloadValue(), partition: '4' };
    expect(buildProduceMessage(value)).toEqual({ value: '', partition: 4 });
  });

  it('drops header rows with a blank key and trims header keys', () => {
    const value = {
      ...createEmptyPayloadValue(),
      headers: [
        { id: '1', key: ' content-type ', value: 'application/json' },
        { id: '2', key: '', value: 'ignored' },
      ],
    };
    expect(buildProduceMessage(value)).toEqual({ value: '', headers: { 'content-type': 'application/json' } });
  });
});
