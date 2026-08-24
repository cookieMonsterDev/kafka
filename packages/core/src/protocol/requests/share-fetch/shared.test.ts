import { describe, expect, it } from 'vitest';
import { Decoder } from '../../decoder';
import { Encoder } from '../../encoder';
import { acknowledgementBatchSchema } from './shared';

describe('protocol/requests/share-fetch/shared', () => {
  it('round-trips an acknowledgement batch, including an empty type list', () => {
    const value = { firstOffset: 10n, lastOffset: 12n, acknowledgeTypes: [1, 2, 3] };
    const encoder = new Encoder();
    acknowledgementBatchSchema.write(encoder, value);
    expect(acknowledgementBatchSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });

  it('round-trips a single-offset batch with no acknowledgement types', () => {
    const value = { firstOffset: 0n, lastOffset: 0n, acknowledgeTypes: [] };
    const encoder = new Encoder();
    acknowledgementBatchSchema.write(encoder, value);
    expect(acknowledgementBatchSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
