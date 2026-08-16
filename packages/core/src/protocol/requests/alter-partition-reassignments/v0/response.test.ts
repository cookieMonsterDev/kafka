import { describe, expect, it } from 'vitest';
import { KafkaAggregateError } from '../../../../errors';
import v0ResponseErrorFixture from '../fixtures/v0-response-error.json' with { type: 'json' };
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { alterPartitionReassignmentsResponseV0 } from './response';

describe('protocol/requests/alter-partition-reassignments/v0/response', () => {
  it('decodes a successful response', async () => {
    const data = await alterPartitionReassignmentsResponseV0.decode(Buffer.from(v0ResponseFixture.data));

    expect(data).toEqual({
      throttleTime: 0,
      errorCode: 0,
      responses: [
        {
          topic: 'test-topic-1',
          partitions: [
            { partition: 1, errorCode: 0 },
            { partition: 0, errorCode: 0 },
          ],
        },
        {
          topic: 'test-topic-2',
          partitions: [{ partition: 0, errorCode: 0 }],
        },
      ],
    });
    await expect(alterPartitionReassignmentsResponseV0.parse(data)).resolves.toBeTruthy();
  });

  it('aggregates per-partition errors', async () => {
    const data = await alterPartitionReassignmentsResponseV0.decode(Buffer.from(v0ResponseErrorFixture.data));

    const promise = alterPartitionReassignmentsResponseV0.parse(data);
    await expect(promise).rejects.toThrow(KafkaAggregateError);
    await expect(promise).rejects.toThrow(
      expect.objectContaining({
        message: 'Errors altering partition reassignments',
        errors: expect.arrayContaining([
          expect.objectContaining({
            message: 'Replica assignment is invalid',
            topic: 'test-topic-f9d6da30a8893d0ec3e9-85563-975cbeab-1fd0-4800-8e69-3b974c21aef6',
            partition: 0,
          }),
        ]),
      }),
    );
  });
});
