import { describe, expect, it } from 'vitest';
import v2RequestFixture from '../fixtures/v2-request.json' with { type: 'json' };
import { listOffsetsRequestV2 } from './request';

describe('protocol/requests/list-offsets/v2/request', () => {
  it('encodes replicaId, isolationLevel and topics/partitions matching a real fixture', async () => {
    const definition = listOffsetsRequestV2({
      replicaId: -1,
      isolationLevel: 0,
      topics: [{ topic: 'test-topic-727705ce68c29fedddf4', partitions: [{ partition: 0, timestamp: 1509285569484n }] }],
    });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v2RequestFixture.data));
  });
});
