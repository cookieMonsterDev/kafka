import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { listTransactionsRequestV1, requestSchema } from './request';

describe('protocol/requests/list-transactions/v1/request', () => {
  it('encodes durationFilter after the v0 filter arrays', async () => {
    const payload = { stateFilters: ['Ongoing'], producerIdFilters: [7n], durationFilter: 1_000n };
    const encoder = await listTransactionsRequestV1(payload).encode();
    const expected = new Encoder()
      .writeUVarInt(2)
      .writeUVarIntString('Ongoing')
      .writeUVarInt(2)
      .writeInt64(7n)
      .writeInt64(1_000n)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('encodes the no-filter duration sentinel', async () => {
    const encoder = await listTransactionsRequestV1({
      stateFilters: [],
      producerIdFilters: [],
      durationFilter: -1n,
    }).encode();
    expect(encoder.buffer).toEqual(
      new Encoder().writeUVarInt(1).writeUVarInt(1).writeInt64(-1n).writeUVarInt(0).buffer,
    );
  });
});
