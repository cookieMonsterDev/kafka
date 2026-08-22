import { describe, expect, it } from 'vitest';
import { Decoder } from '../../decoder';
import { ShareGroupHeartbeat } from './index';
import { requestSchema } from './v1/request';

describe('protocol/requests/share-group-heartbeat', () => {
  it('implements version 1', () => {
    expect(ShareGroupHeartbeat.versions).toEqual([1]);
  });

  it('creates a version 1 request with subscribed topics', async () => {
    const { request } = ShareGroupHeartbeat.protocol({ version: 1 })({
      groupId: 'g',
      memberId: 'm',
      memberEpoch: 0,
      subscribedTopicNames: ['events'],
    });
    expect(request).toMatchObject({ apiKey: 76, apiVersion: 1, apiName: 'ShareGroupHeartbeat' });

    const encoder = await request.encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual({
      groupId: 'g',
      memberId: 'm',
      memberEpoch: 0,
      rackId: null,
      subscribedTopicNames: ['events'],
    });
  });
});
