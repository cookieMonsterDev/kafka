import { describe, expect, it } from 'vitest';
import v1RequestFixture from '../fixtures/v1-request.json' with { type: 'json' };
import { listOffsetsRequestV1 } from './request.js';

describe('protocol/requests/list-offsets/v1/request', () => {
  it('encodes replicaId and topics/partitions matching a real fixture', async () => {
    const definition = listOffsetsRequestV1({
      replicaId: -1,
      topics: [
        {
          topic: 'test-topic-173c0e1556dab8d50ee6-91677-379faf0f-a357-408e-bd1d-5fa11893b05d',
          partitions: [{ partition: 0, timestamp: 1509285569484n }],
        },
      ],
    });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v1RequestFixture.data));
  });
});
