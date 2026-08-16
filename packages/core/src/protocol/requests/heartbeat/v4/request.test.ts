import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { heartbeatRequestV3 } from '../v3/request';
import { heartbeatRequestV4, requestSchema } from './request';

const payload = {
  groupId: 'g',
  groupGenerationId: 1,
  memberId: 'm',
  groupInstanceId: 'instance-1' as string | null,
};

describe('protocol/requests/heartbeat/v4/request', () => {
  it('encodes compact strings and a TAG_BUFFER', async () => {
    const definition = heartbeatRequestV4(payload);
    expect(definition.apiVersion).toBe(4);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarIntString('g')
      .writeInt32(1)
      .writeUVarIntString('m')
      .writeUVarIntString('instance-1')
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('is not the non-flexible v3 encoding', async () => {
    const v4 = await heartbeatRequestV4(payload).encode();
    const v3 = await heartbeatRequestV3(payload).encode();
    expect(v4.buffer).not.toEqual(v3.buffer);
  });
});
