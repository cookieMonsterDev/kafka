import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { findCoordinatorRequestV4, requestSchema } from './request';

const payload = { coordinatorType: 1, coordinatorKeys: ['tx-a', 'tx-b'] };

describe('protocol/requests/find-coordinator/v4/request', () => {
  it('encodes the key type before a compact array of keys and a trailing TAG_BUFFER', async () => {
    const definition = findCoordinatorRequestV4(payload);
    expect(definition.apiVersion).toBe(4);
    expect(definition.apiName).toBe('GroupCoordinator');

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeInt8(1)
      .writeUVarInt(3)
      .writeUVarIntString('tx-a')
      .writeUVarIntString('tx-b')
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });
});
