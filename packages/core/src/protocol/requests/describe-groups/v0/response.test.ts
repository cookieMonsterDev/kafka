import { describe, expect, it } from 'vitest';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { describeGroupsResponseV0 } from './response';

describe('protocol/requests/describe-groups/v0/response', () => {
  it('decodes a real fixture', async () => {
    const data = await describeGroupsResponseV0.decode(Buffer.from(v0ResponseFixture.data));

    expect(data).toEqual({
      groups: [
        {
          errorCode: 0,
          groupId: 'consumer-group-id-608e7e42043d917ecb44',
          state: 'Stable',
          protocolType: 'consumer',
          protocol: 'default',
          members: [
            {
              memberId: 'test-5cc3bc27ca2660144976-fec6ade1-82ef-461e-81fe-c30e5908e2d2',
              clientId: 'test-5cc3bc27ca2660144976',
              clientHost: '/172.18.0.1',
              memberMetadata: Buffer.from([0, 0]),
              memberAssignment: Buffer.from('{}'),
            },
          ],
        },
      ],
    });
    await expect(describeGroupsResponseV0.parse(data)).resolves.toBeTruthy();
  });

  it('throws using the first errored group', async () => {
    const data = {
      groups: [
        { errorCode: 0, groupId: 'ok', state: '', protocolType: '', protocol: '', members: [] },
        { errorCode: 35, groupId: 'bad', state: '', protocolType: '', protocol: '', members: [] },
      ],
    };
    await expect(describeGroupsResponseV0.parse(data)).rejects.toThrow(/version of API is not supported/);
  });
});
