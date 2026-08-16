import { describe, expect, it } from 'vitest';
import v1RequestFixture from '../fixtures/v1-request.json' with { type: 'json' };
import { offsetFetchRequestV1 } from './request';

describe('protocol/requests/offset-fetch/v1/request', () => {
  it('encodes groupId and topics matching a real fixture', async () => {
    const definition = offsetFetchRequestV1({
      groupId: 'consumer-group-id-c7dcb2473b6a1196b2b2',
      topics: [{ topic: 'test-topic-9f9b074057acd4335946', partitions: [{ partition: 0 }] }],
    });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v1RequestFixture.data));
  });
});
