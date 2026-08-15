import { describe, expect, it } from 'vitest';
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { alterPartitionReassignmentsRequestV0 } from './request.js';

describe('protocol/requests/alter-partition-reassignments/v0/request', () => {
  it('encodes topics and their partition reassignments', async () => {
    const definition = alterPartitionReassignmentsRequestV0({
      topics: [
        {
          topic: 'test-topic-1',
          partitionAssignment: [
            { partition: 0, replicas: [0, 1] },
            { partition: 1, replicas: [1, 2] },
          ],
        },
        {
          topic: 'test-topic-2',
          partitionAssignment: [{ partition: 0, replicas: [0, 2] }],
        },
      ],
      timeout: 30000,
    });
    const encoder = await definition.encode();

    expect(encoder.buffer).toEqual(Buffer.from(v0RequestFixture.data));
  });
});
