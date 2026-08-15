import { describe, expect, it } from 'vitest';
import v2RequestFixture from '../fixtures/v2-request.json' with { type: 'json' };
import { offsetCommitRequestV2 } from './request.js';

describe('protocol/requests/offset-commit/v2/request', () => {
  it('encodes groupId, memberId, retentionTime and topics matching a real fixture', async () => {
    const definition = offsetCommitRequestV2({
      groupId: 'consumer-group-id-3a1646e3e927e05cd0c2',
      groupGenerationId: 1,
      memberId: 'test-8aac10296d949b162708-6ff63ddf-1a5a-4f05-929c-17158875aa7f',
      retentionTime: -1n,
      topics: [
        { topic: 'test-topic-9167000121c242c36142', partitions: [{ partition: 0, offset: 0n, metadata: null }] },
      ],
    });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v2RequestFixture.data));
  });
});
