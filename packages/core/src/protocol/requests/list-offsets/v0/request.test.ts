import { describe, expect, it } from 'vitest';
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { listOffsetsRequestV0 } from './request';

describe('protocol/requests/list-offsets/v0/request', () => {
  it('encodes replicaId, timestamp and maxNumOffsets matching a real fixture', async () => {
    const definition = listOffsetsRequestV0({
      replicaId: -1,
      topics: [
        {
          topic: 'test-topic-727705ce68c29fedddf4',
          partitions: [{ partition: 0, timestamp: 1509285569484n, maxNumOffsets: 1 }],
        },
      ],
    });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v0RequestFixture.data));
  });
});
