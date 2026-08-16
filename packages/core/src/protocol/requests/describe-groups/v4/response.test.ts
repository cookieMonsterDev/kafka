import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { describeGroupsResponseV4 } from './response';

const memberMetadata = Buffer.from([0, 0]);
const memberAssignment = Buffer.from('{}');

function encodeV4Response(options: { throttleTime: number }): Buffer {
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
    .writeString('instance-1')
    .writeString('client-1')
    .writeString('/127.0.0.1')
    .writeBytes(memberMetadata)
    .writeBytes(memberAssignment)
    .writeInt32(0).buffer;
}

describe('protocol/requests/describe-groups/v4/response', () => {
  it('decodes groupInstanceId on each member and remaps throttleTime', async () => {
    const data = await describeGroupsResponseV4.decode(encodeV4Response({ throttleTime: 4 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 4,
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
              groupInstanceId: 'instance-1',
              clientId: 'client-1',
              clientHost: '/127.0.0.1',
              memberMetadata,
              memberAssignment,
            },
          ],
          authorizedOperations: 0,
        },
      ],
    });
    await expect(describeGroupsResponseV4.parse(data)).resolves.toBe(data);
  });
});
