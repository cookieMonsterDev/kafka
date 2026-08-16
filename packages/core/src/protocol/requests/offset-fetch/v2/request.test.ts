import { describe, expect, it } from 'vitest';
import v2RequestFixture from '../fixtures/v2-request.json' with { type: 'json' };
import { offsetFetchRequestV2 } from './request';

describe('protocol/requests/offset-fetch/v2/request', () => {
  it('encodes matching a real fixture', async () => {
    const definition = offsetFetchRequestV2({
      groupId: 'consumer-group-id-c7dcb2473b6a1196b2b2',
      topics: [{ topic: 'test-topic-9f9b074057acd4335946', partitions: [{ partition: 0 }] }],
    });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v2RequestFixture.data));
  });
});
