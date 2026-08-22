import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { listTransactionsRequestV2, requestSchema } from './request';

describe('protocol/requests/list-transactions/v2/request', () => {
  it('encodes a transactional ID pattern after the v1 fields', async () => {
    const payload = {
      stateFilters: [],
      producerIdFilters: [],
      durationFilter: -1n,
      transactionalIdPattern: 'payments-*',
    };
    const encoder = await listTransactionsRequestV2(payload).encode();
    const expected = new Encoder()
      .writeUVarInt(1)
      .writeUVarInt(1)
      .writeInt64(-1n)
      .writeUVarIntString('payments-*')
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('encodes a null transactional ID pattern', async () => {
    const payload = {
      stateFilters: [],
      producerIdFilters: [],
      durationFilter: -1n,
      transactionalIdPattern: null,
    };
    const encoder = await listTransactionsRequestV2(payload).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });
});
