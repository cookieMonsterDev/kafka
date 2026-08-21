import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { listTransactionsRequestV0, requestSchema } from './request';

describe('protocol/requests/list-transactions/v0/request', () => {
  it('encodes state and producer ID filters as compact arrays', async () => {
    const payload = { stateFilters: ['Ongoing', 'Empty'], producerIdFilters: [42n, 99n] };
    const encoder = await listTransactionsRequestV0(payload).encode();
    const expected = new Encoder()
      .writeUVarInt(3)
      .writeUVarIntString('Ongoing')
      .writeUVarIntString('Empty')
      .writeUVarInt(3)
      .writeInt64(42n)
      .writeInt64(99n)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('encodes empty filters as compact empty arrays', async () => {
    const encoder = await listTransactionsRequestV0({ stateFilters: [], producerIdFilters: [] }).encode();
    expect(encoder.buffer).toEqual(new Encoder().writeUVarInt(1).writeUVarInt(1).writeUVarInt(0).buffer);
  });
});
