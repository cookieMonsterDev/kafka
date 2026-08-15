import { describe, expect, it } from 'vitest';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { addPartitionsToTxnResponseV0 } from './response.js';

describe('protocol/requests/add-partitions-to-txn/v0/response', () => {
  it('decodes a real fixture', async () => {
    const data = await addPartitionsToTxnResponseV0.decode(Buffer.from(v0ResponseFixture.data));

    expect(data).toEqual({
      throttleTime: 0,
      errors: [
        {
          topic: 'test-topic-f6bab978bdca094688e3-37015-ca4f7ad4-5dcc-4bb9-9853-b1e4c4ed78a7',
          partitionErrors: [
            { errorCode: 0, partition: 1 },
            { errorCode: 0, partition: 2 },
          ],
        },
      ],
    });
    await expect(addPartitionsToTxnResponseV0.parse(data)).resolves.toBeTruthy();
  });

  it('throws if there is an error on any of the partitions', async () => {
    const data = {
      throttleTime: 0,
      errors: [
        {
          topic: 'test-topic',
          partitionErrors: [
            { errorCode: 0, partition: 1 },
            { errorCode: 49, partition: 2 },
          ],
        },
      ],
    };

    await expect(addPartitionsToTxnResponseV0.parse(data)).rejects.toThrow(
      /producer id which is not currently assigned to its transactional id/,
    );
  });
});
