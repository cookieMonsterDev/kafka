import { describe, expect, it } from 'vitest';
import { ShareAcknowledge } from './index';

const topicId = Buffer.from('0123456789abcdef');

describe('protocol/requests/share-acknowledge', () => {
  it('implements versions 1-2', () => {
    expect(ShareAcknowledge.versions).toEqual([1, 2]);
  });

  it('builds a version 1 request', () => {
    const { request } = ShareAcknowledge.protocol({ version: 1 })({
      groupId: 'g',
      memberId: 'm',
      shareSessionEpoch: 1,
      topics: [
        {
          topicId,
          partitions: [
            {
              partitionIndex: 0,
              acknowledgementBatches: [{ firstOffset: 1n, lastOffset: 1n, acknowledgeTypes: [1] }],
            },
          ],
        },
      ],
    });
    expect(request).toMatchObject({ apiKey: 79, apiVersion: 1, apiName: 'ShareAcknowledge' });
  });

  it('builds a version 2 request', () => {
    const { request } = ShareAcknowledge.protocol({ version: 2 })({
      groupId: 'g',
      memberId: 'm',
      shareSessionEpoch: 1,
      isRenewAck: true,
      topics: [
        {
          topicId,
          partitions: [
            {
              partitionIndex: 0,
              acknowledgementBatches: [{ firstOffset: 1n, lastOffset: 1n, acknowledgeTypes: [4] }],
            },
          ],
        },
      ],
    });
    expect(request).toMatchObject({ apiKey: 79, apiVersion: 2, apiName: 'ShareAcknowledge' });
  });
});
