import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { syncGroupResponseV5 } from './response';

const memberAssignment = Buffer.from('assign');

function encodeV5Response(options: { throttleTime: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeInt16(0)
    .writeUVarIntString('consumer')
    .writeUVarIntString('AssignerName')
    .writeUVarIntBytes(memberAssignment)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/sync-group/v5/response', () => {
  it('decodes protocol type/name and keeps memberAssignment', async () => {
    const data = await syncGroupResponseV5.decode(encodeV5Response({ throttleTime: 8 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      errorCode: 0,
      protocolType: 'consumer',
      protocolName: 'AssignerName',
      memberAssignment,
    });
    await expect(syncGroupResponseV5.parse(data)).resolves.toBe(data);
  });
});
