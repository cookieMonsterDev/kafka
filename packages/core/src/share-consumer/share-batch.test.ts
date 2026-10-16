import { describe, expect, it } from 'vitest';
import type { KafkaMessage } from '../consumer/types';
import { ShareBatch } from './share-batch';

function dataMessage(offset: bigint): KafkaMessage {
  return {
    magicByte: 2,
    attributes: 0,
    timestamp: 1n,
    offset,
    key: null,
    value: Buffer.from('x'),
    headers: {},
    isControlRecord: false,
    batchContext: {
      firstOffset: 0n,
      firstTimestamp: 1n,
      partitionLeaderEpoch: 0,
      inTransaction: false,
      isControlBatch: false,
      lastOffsetDelta: 5,
      producerId: -1n,
      producerEpoch: -1,
      firstSequence: -1,
      maxTimestamp: 1n,
      timestampType: 0,
      magicByte: 2,
    },
    byteSize: 1,
  };
}

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
          byteSize: 1,
        },
      ],
      acquiredRecords: [{ firstOffset: 1n, lastOffset: 1n, deliveryCount: 1 }],
    });
    expect(batch.isEmpty()).toBe(true);
    expect(batch.acquiredRecords).toHaveLength(1);
  });

  it('keeps only records inside the acquired ranges', () => {
    const batch = new ShareBatch('events', {
      partition: 0,
      messages: [0n, 1n, 2n, 3n, 4n, 5n].map(dataMessage),
      acquiredRecords: [
        { firstOffset: 1n, lastOffset: 2n, deliveryCount: 1 },
        { firstOffset: 4n, lastOffset: 4n, deliveryCount: 2 },
      ],
    });
    expect(batch.messages.map(({ offset }) => offset)).toEqual([1n, 2n, 4n]);
  });
});
