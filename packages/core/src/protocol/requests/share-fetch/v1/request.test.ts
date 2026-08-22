import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { shareFetchRequestV1, requestSchema } from './request';

const topicId = Buffer.from('0123456789abcdef');

describe('protocol/requests/share-fetch/v1/request', () => {
  it('round-trips a share fetch request with acknowledgement batches', async () => {
    const value = {
      groupId: 'my-group',
      memberId: 'member-1',
      shareSessionEpoch: 1,
      maxWaitMs: 500,
      minBytes: 1,
      maxBytes: 1_048_576,
      maxRecords: 100,
      batchSize: 10,
      topics: [
        {
          topicId,
          partitions: [
            {
              partitionIndex: 0,
              acknowledgementBatches: [{ firstOffset: 10n, lastOffset: 12n, acknowledgeTypes: [1, 1, 1] }],
            },
          ],
        },
      ],
      forgottenTopics: [{ topicId, partitions: [1] }],
    };

    const encoder = await shareFetchRequestV1(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
