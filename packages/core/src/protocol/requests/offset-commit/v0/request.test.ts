import { describe, expect, it } from 'vitest';
import v0RequestMetadataFixture from '../fixtures/v0-request-metadata.json' with { type: 'json' };
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { offsetCommitRequestV0 } from './request';

describe('protocol/requests/offset-commit/v0/request', () => {
  it('encodes groupId and topics matching a real fixture', async () => {
    const definition = offsetCommitRequestV0({
      groupId: 'consumer-group-id-9ea5b85471316d2753ab',
      topics: [
        { topic: 'test-topic-eb1a285cda2e9f9a1021', partitions: [{ partition: 0, offset: 0n, metadata: null }] },
      ],
    });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v0RequestFixture.data));
  });

  it('encodes optional metadata', async () => {
    const definition = offsetCommitRequestV0({
      groupId: 'consumer-group-id-9ea5b85471316d2753ab',
      topics: [
        { topic: 'test-topic-eb1a285cda2e9f9a1021', partitions: [{ partition: 0, offset: 0n, metadata: 'test' }] },
      ],
    });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v0RequestMetadataFixture.data));
  });
});
