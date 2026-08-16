import { describe, expect, it } from 'vitest';
import v2RequestFixture from '../fixtures/v2-request.json' with { type: 'json' };
import { listOffsetsRequestV3 } from './request';

describe('protocol/requests/list-offsets/v3/request', () => {
  it('encodes identically to v2, wire-for-wire', async () => {
    const definition = listOffsetsRequestV3({
      replicaId: -1,
      isolationLevel: 0,
      topics: [{ topic: 'test-topic-727705ce68c29fedddf4', partitions: [{ partition: 0, timestamp: 1509285569484n }] }],
    });
    expect(definition.apiVersion).toBe(3);
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v2RequestFixture.data));
  });
});
