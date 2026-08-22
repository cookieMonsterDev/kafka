import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { requestSchema, shareFetchRequestV2 } from './request';

const topicId = Buffer.from('0123456789abcdef');

describe('protocol/requests/share-fetch/v2/request', () => {
  it('round-trips shareAcquireMode and isRenewAck', async () => {
    const value = {
      groupId: 'my-group',
      memberId: 'member-1',
      shareSessionEpoch: 1,
      maxWaitMs: 500,
      minBytes: 1,
      maxBytes: 1_048_576,
      maxRecords: 100,
      batchSize: 10,
      shareAcquireMode: 1,
      isRenewAck: true,
      topics: [
        {
          topicId,
          partitions: [
            {
              partitionIndex: 0,
              acknowledgementBatches: [{ firstOffset: 10n, lastOffset: 12n, acknowledgeTypes: [4] }],
            },
          ],
        },
      ],
      forgottenTopics: [],
    };

    const encoder = await shareFetchRequestV2(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
