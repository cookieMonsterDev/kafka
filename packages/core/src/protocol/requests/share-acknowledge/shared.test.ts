import { describe, expect, it } from 'vitest';
import { Decoder } from '../../decoder';
import { Encoder } from '../../encoder';
import { acknowledgementBatchSchema } from './shared';

describe('protocol/requests/share-acknowledge/shared', () => {
  it('round-trips an acknowledgement batch', () => {
    const value = { firstOffset: 4n, lastOffset: 9n, acknowledgeTypes: [1, 1, 3] };
    const encoder = new Encoder();
    acknowledgementBatchSchema.write(encoder, value);
    expect(acknowledgementBatchSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
