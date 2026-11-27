import { describe, expect, it } from 'vitest';
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { alterPartitionReassignmentsRequestV1 } from './request';

const topics = [
  {
    topic: 'test-topic-1',
    partitionAssignment: [
      { partition: 0, replicas: [0, 1] },
      { partition: 1, replicas: [1, 2] },
    ],
  },
  { topic: 'test-topic-2', partitionAssignment: [{ partition: 0, replicas: [0, 2] }] },
];

/** v0's body with the v1 boolean inserted after `timeout_ms`. */
function expectedBody(allowReplicationFactorChange: boolean): Buffer {
  const v0 = Buffer.from(v0RequestFixture.data);
  return Buffer.concat([v0.subarray(0, 4), Buffer.from([allowReplicationFactorChange ? 1 : 0]), v0.subarray(4)]);
}

describe('protocol/requests/alter-partition-reassignments/v1/request', () => {
  it('writes allow_replication_factor_change after timeout_ms', async () => {
    const definition = alterPartitionReassignmentsRequestV1({
      topics,
      timeout: 30000,
      allowReplicationFactorChange: false,
    });

    expect(definition.apiVersion).toBe(1);
    expect((await definition.encode()).buffer).toEqual(expectedBody(false));
  });

  it('allows replication factor changes by default', async () => {
    const encoder = await alterPartitionReassignmentsRequestV1({ topics, timeout: 30000 }).encode();
    expect(encoder.buffer).toEqual(expectedBody(true));
  });
});
