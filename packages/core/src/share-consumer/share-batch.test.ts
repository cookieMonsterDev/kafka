import { describe, expect, it } from 'vitest';
import { ShareBatch } from './share-batch';

describe('share-consumer/share-batch', () => {
  it('drops control records and reports empty when none remain', () => {
    const batch = new ShareBatch('events', {
      partition: 0,
      messages: [
        {
          magicByte: 2,
          attributes: 0,
          timestamp: 1n,
          offset: 1n,
          key: null,
          value: Buffer.from('x'),
          headers: {},
          isControlRecord: true,
          batchContext: {
            firstOffset: 1n,
            firstTimestamp: 1n,
            partitionLeaderEpoch: 0,
            inTransaction: false,
            isControlBatch: true,
            lastOffsetDelta: 0,
            producerId: -1n,
            producerEpoch: -1,
            firstSequence: -1,
            maxTimestamp: 1n,
            timestampType: 0,
            magicByte: 2,
          },
        },
      ],
      acquiredRecords: [{ firstOffset: 1n, lastOffset: 1n, deliveryCount: 1 }],
    });
    expect(batch.isEmpty()).toBe(true);
    expect(batch.acquiredRecords).toHaveLength(1);
  });
});
