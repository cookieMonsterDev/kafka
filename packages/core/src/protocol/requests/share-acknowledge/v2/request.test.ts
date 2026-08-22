import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { requestSchema, shareAcknowledgeRequestV2 } from './request';

const topicId = Buffer.from('0123456789abcdef');

describe('protocol/requests/share-acknowledge/v2/request', () => {
  it('round-trips isRenewAck', async () => {
    const value = {
      groupId: 'my-group',
      memberId: 'member-1',
      shareSessionEpoch: 1,
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
    };

    const encoder = await shareAcknowledgeRequestV2(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
