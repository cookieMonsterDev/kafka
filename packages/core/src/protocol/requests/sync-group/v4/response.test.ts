import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { syncGroupResponseV4 } from './response';

const memberAssignment = Buffer.from('assign');

function encodeV4Response(options: { throttleTime: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeInt16(0)
    .writeUVarIntBytes(memberAssignment)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/sync-group/v4/response', () => {
  it('decodes a compact assignment and remaps throttleTime', async () => {
    const data = await syncGroupResponseV4.decode(encodeV4Response({ throttleTime: 8 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      errorCode: 0,
      memberAssignment,
    });
    await expect(syncGroupResponseV4.parse(data)).resolves.toBe(data);
  });
});
