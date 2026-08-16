import { describe, expect, it } from 'vitest';
import v2AssignerMetadataFixture from '../fixtures/v2-assigner-metadata.json' with { type: 'json' };
import v5RequestFixture from '../fixtures/v5-request.json' with { type: 'json' };
import { joinGroupRequestV5 } from './request';

describe('protocol/requests/join-group/v5/request', () => {
  it('encodes to match the real fixture, including group_instance_id', async () => {
    const definition = joinGroupRequestV5({
      groupId: 'consumer-group-id-b522188a3a12a1f04cfb-23702-e1ff35c7-fde9-4d58-960a-2cef8af77eef',
      sessionTimeout: 30000,
      rebalanceTimeout: 60000,
      memberId: '',
      groupInstanceId: 'group-instance-id',
      protocolType: 'consumer',
      groupProtocols: [{ name: 'AssignerName', metadata: Buffer.from(v2AssignerMetadataFixture.data) }],
    });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v5RequestFixture.data));
  });
});
