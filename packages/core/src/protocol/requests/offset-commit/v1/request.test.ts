import { describe, expect, it } from 'vitest';
import v1RequestMetadataFixture from '../fixtures/v1-request-metadata.json' with { type: 'json' };
import v1RequestFixture from '../fixtures/v1-request.json' with { type: 'json' };
import { offsetCommitRequestV1 } from './request';

describe('protocol/requests/offset-commit/v1/request', () => {
  it('encodes groupId, memberId, timestamp and topics matching a real fixture', async () => {
    const definition = offsetCommitRequestV1({
      groupId: 'consumer-group-id-25c9a1474733b283e6c6',
      groupGenerationId: 1,
      memberId: 'test-d001f2e7c1d704ed30f7-1cf32daa-64e3-4305-a0a5-db4088dfb4eb',
      topics: [
        {
          topic: 'test-topic-9c1581c756889e8773dd',
          partitions: [{ partition: 0, offset: 0n, timestamp: 1509292875164n, metadata: null }],
        },
      ],
    });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v1RequestFixture.data));
  });

  it('encodes optional metadata', async () => {
    const definition = offsetCommitRequestV1({
      groupId: 'consumer-group-id-25c9a1474733b283e6c6',
      groupGenerationId: 1,
      memberId: 'test-d001f2e7c1d704ed30f7-1cf32daa-64e3-4305-a0a5-db4088dfb4eb',
      topics: [
        {
          topic: 'test-topic-9c1581c756889e8773dd',
          partitions: [{ partition: 0, offset: 0n, timestamp: 1509292875164n, metadata: 'test' }],
        },
      ],
    });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v1RequestMetadataFixture.data));
  });
});
