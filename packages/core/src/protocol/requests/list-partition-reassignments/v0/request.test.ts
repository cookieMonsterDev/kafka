import { describe, expect, it } from 'vitest';
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { listPartitionReassignmentsRequestV0 } from './request.js';

describe('protocol/requests/list-partition-reassignments/v0/request', () => {
  it('encodes the requested topics and their partitions', async () => {
    const definition = listPartitionReassignmentsRequestV0({
      topics: [
        {
          topic: 'test-topic-1f131dd7f83b8d72a447-33298-d13ec602-1a34-41c8-b59e-0657aef3ad25',
          partitions: [0],
        },
      ],
      timeout: 5000,
    });
    const encoder = await definition.encode();

    expect(encoder.buffer).toEqual(Buffer.from(v0RequestFixture.data));
  });
});
