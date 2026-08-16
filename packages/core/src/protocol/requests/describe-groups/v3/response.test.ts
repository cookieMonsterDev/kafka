import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { describeGroupsResponseV3 } from './response';

const memberMetadata = Buffer.from([0, 0]);
const memberAssignment = Buffer.from('{}');

function encodeV3Response(options: { throttleTime: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeInt32(1)
    .writeInt16(0)
    .writeString('g1')
    .writeString('Stable')
    .writeString('consumer')
    .writeString('range')
    .writeInt32(1)
    .writeString('member-1')
    .writeString('client-1')
    .writeString('/127.0.0.1')
    .writeBytes(memberMetadata)
    .writeBytes(memberAssignment)
    .writeInt32(-2147483648).buffer;
}

describe('protocol/requests/describe-groups/v3/response', () => {
  it('decodes authorizedOperations on each group and remaps throttleTime', async () => {
    const data = await describeGroupsResponseV3.decode(encodeV3Response({ throttleTime: 8 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      groups: [
        {
          errorCode: 0,
          groupId: 'g1',
          state: 'Stable',
          protocolType: 'consumer',
          protocol: 'range',
          members: [
            {
              memberId: 'member-1',
              clientId: 'client-1',
              clientHost: '/127.0.0.1',
              memberMetadata,
              memberAssignment,
            },
          ],
          authorizedOperations: -2147483648,
        },
      ],
    });
    await expect(describeGroupsResponseV3.parse(data)).resolves.toBe(data);
  });
});
