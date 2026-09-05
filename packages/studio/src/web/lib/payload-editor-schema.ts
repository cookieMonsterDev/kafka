import type { ProduceMessage } from '../../shared/contracts/produce';

export type ValueFormat = 'json' | 'text';

export interface HeaderRow {
  readonly id: string;
  readonly key: string;
  readonly value: string;
}

export interface PayloadEditorValue {
  readonly key: string;
  readonly value: string;
  readonly valueFormat: ValueFormat;
  readonly partition: string;
  readonly headers: readonly HeaderRow[];
}

export function createEmptyPayloadValue(): PayloadEditorValue {
  return { key: '', value: '', valueFormat: 'json', partition: '', headers: [] };
}

/**
 * One error for the whole editor rather than per-field — this form only ever has one or two things
 * that can be wrong at a time, and a single message is easier to scan than a field-by-field list.
 */
export function payloadEditorValueError(value: PayloadEditorValue): string | null {
  if (value.valueFormat === 'json' && value.value.trim() !== '') {
    try {
      JSON.parse(value.value);
    } catch (error) {
      return `Value is not valid JSON: ${error instanceof Error ? error.message : String(error)}`;
    }
  }
  if (value.partition.trim() !== '' && !/^\d+$/.test(value.partition.trim())) {
    return 'Partition must be a non-negative whole number';
  }
  return null;
}

/** Builds the wire message for either a single send or a burst template — the caller decides which. */
export function buildProduceMessage(value: PayloadEditorValue): ProduceMessage {
  const headers = Object.fromEntries(
    value.headers.filter((row) => row.key.trim().length > 0).map((row) => [row.key.trim(), row.value]),
  );
  return {
    ...(value.key.trim() !== '' ? { key: value.key } : {}),
    value: value.value,
    ...(value.partition.trim() !== '' ? { partition: Number(value.partition) } : {}),
    ...(Object.keys(headers).length > 0 ? { headers } : {}),
  };
}
