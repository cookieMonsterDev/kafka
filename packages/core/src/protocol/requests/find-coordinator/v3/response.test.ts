import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { findCoordinatorResponseV3 } from './response';

function encodeV3Response(options: { throttleTime: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeInt16(0)
    .writeUVarIntString(null)
    .writeInt32(2)
    .writeUVarIntString('192.168.50.211')
    .writeInt32(9098)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/find-coordinator/v3/response', () => {
  it('decodes a flexible body and remaps throttleTime', async () => {
    const data = await findCoordinatorResponseV3.decode(encodeV3Response({ throttleTime: 8 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      errorCode: 0,
      errorMessage: null,
      coordinator: { nodeId: 2, host: '192.168.50.211', port: 9098 },
    });
    await expect(findCoordinatorResponseV3.parse(data)).resolves.toBe(data);
  });
});
