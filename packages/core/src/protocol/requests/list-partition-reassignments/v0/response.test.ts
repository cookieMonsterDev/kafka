import { describe, expect, it } from 'vitest';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { listPartitionReassignmentsResponseV0 } from './response.js';

describe('protocol/requests/list-partition-reassignments/v0/response', () => {
  it('decodes topics with their replica reassignment state', async () => {
    const data = await listPartitionReassignmentsResponseV0.decode(Buffer.from(v0ResponseFixture.data));

    expect(data).toEqual({
      throttleTime: 0,
      errorCode: 0,
      topics: [
        {
          name: 'test-topic-1f131dd7f83b8d72a447-33298-d13ec602-1a34-41c8-b59e-0657aef3ad25',
          partitions: [{ partition: 0, replicas: [2, 1, 0], addingReplicas: [1], removingReplicas: [0] }],
        },
      ],
    });
    await expect(listPartitionReassignmentsResponseV0.parse(data)).resolves.toBeTruthy();
  });
});
