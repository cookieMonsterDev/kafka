import { describe, expect, it } from 'vitest';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { txnOffsetCommitResponseV0 } from './response.js';

describe('protocol/requests/txn-offset-commit/v0/response', () => {
  it('decodes a real fixture', async () => {
    const data = await txnOffsetCommitResponseV0.decode(Buffer.from(v0ResponseFixture.data));

    expect(data).toEqual({
      throttleTime: 0,
      topics: [
        {
          topic: 'test-topic-0ba33173f7664d75c6b2-63632-a0dab079-1c9a-44ba-be25-ca3d50df5003',
          partitions: [
            { errorCode: 0, partition: 1 },
            { errorCode: 0, partition: 2 },
          ],
        },
      ],
    });
    await expect(txnOffsetCommitResponseV0.parse(data)).resolves.toBeTruthy();
  });

  it('throws if there is an error on any of the partitions', async () => {
    const data = {
      throttleTime: 0,
      topics: [
        {
          topic: 'test-topic',
          partitions: [
            { errorCode: 0, partition: 1 },
            { errorCode: 49, partition: 2 },
          ],
        },
      ],
    };

    await expect(txnOffsetCommitResponseV0.parse(data)).rejects.toThrow(
      /producer id which is not currently assigned to its transactional id/,
    );
  });
});
