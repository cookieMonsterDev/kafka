import { describe, expect, it } from 'vitest';
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { deleteRecordsRequestV1 } from './request.js';

describe('protocol/requests/delete-records/v1/request', () => {
  it('encodes to the same wire format as v0', async () => {
    const encoder = await deleteRecordsRequestV1({
      topics: [
        {
          topic: 'test-topic-42132ca1c79e5dd6c436-81884-14d3a181-013d-4176-8e7e-7518a67f4813',
          partitions: [{ partition: 0, offset: 7n }],
        },
      ],
      timeout: 5000,
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v0RequestFixture.data));
  });
});
