import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { shareAcknowledgeRequestV1, requestSchema } from './request';

const topicId = Buffer.from('0123456789abcdef');

describe('protocol/requests/share-acknowledge/v1/request', () => {
  it('round-trips a share acknowledge request', async () => {
    const value = {
      groupId: 'my-group',
      memberId: 'member-1',
      shareSessionEpoch: 1,
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
    };

    const encoder = await shareAcknowledgeRequestV1(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
