import { describe, expect, it } from 'vitest';
import { TIMESTAMP_TYPES } from '../protocol/enums/timestamp-types';
import { Batch } from './batch';
import type { KafkaMessage } from './types';

const defaultBatchContext = {
  firstOffset: 0n,
  firstTimestamp: 0n,
  partitionLeaderEpoch: 0,
  inTransaction: false,
  isControlBatch: false,
  lastOffsetDelta: 0,
  producerId: -1n,
  producerEpoch: 0,
  firstSequence: 0,
  maxTimestamp: 0n,
  timestampType: TIMESTAMP_TYPES.CREATE_TIME,
  magicByte: 2,
};

function msg(offset: bigint, extra: Partial<KafkaMessage> = {}): KafkaMessage {
  return {
    magicByte: 2,
    attributes: 0,
    timestamp: 0n,
    offset,
    key: extra.key ?? null,
    value: extra.value ?? null,
    headers: {},
    isControlRecord: extra.isControlRecord ?? false,
    batchContext: extra.batchContext ?? defaultBatchContext,
    ...extra,
  };
}

describe('consumer/batch', () => {
  const topic = 'topic-name';

  it('discards messages with a lower offset than the requested', () => {
    const batch = new Batch(topic, 3n, {
      partition: 0,
      highWatermark: 100n,
      messages: [msg(0n), msg(1n), msg(2n), msg(3n), msg(4n), msg(5n)],
    });

    expect(batch.messages.map((m) => m.offset)).toEqual([3n, 4n, 5n]);
  });

  it('discards control records', () => {
    const batch = new Batch(topic, 0n, {
      partition: 0,
      highWatermark: 100n,
      messages: [msg(3n, { isControlRecord: true }), msg(4n), msg(5n)],
    });

    expect(batch.messages.map((m) => m.offset)).toEqual([4n, 5n]);
  });

  describe('#isEmpty', () => {
    it('returns true when empty', () => {
      const batch = new Batch(topic, 0n, { partition: 0, highWatermark: 100n, messages: [] });
      expect(batch.isEmpty()).toEqual(true);
    });

    it('returns false when it has messages', () => {
      const batch = new Batch(topic, 0n, { partition: 0, highWatermark: 100n, messages: [msg(1n), msg(2n)] });
      expect(batch.isEmpty()).toEqual(false);
    });
  });

  describe('#firstOffset', () => {
    it('returns the offset of the first message', () => {
      const batch = new Batch(topic, 0n, { partition: 0, highWatermark: 100n, messages: [msg(1n), msg(2n)] });
      expect(batch.firstOffset()).toEqual(1n);
    });

    it('returns null when the batch is empty', () => {
      const batch = new Batch(topic, 0n, { partition: 0, highWatermark: 100n, messages: [] });
      expect(batch.firstOffset()).toEqual(null);
    });
  });

  describe('#lastOffset', () => {
    it('returns the offset of the last message', () => {
      const batch = new Batch(topic, 0n, { partition: 0, highWatermark: 100n, messages: [msg(1n), msg(2n)] });
      expect(batch.lastOffset()).toEqual(2n);
    });

    it('returns highWatermark - 1 when the batch is empty', () => {
      const batch = new Batch(topic, 0n, { partition: 0, highWatermark: 100n, messages: [] });
      expect(batch.lastOffset()).toEqual(99n);
    });
  });

  describe('#offsetLag', () => {
    it('returns the difference between highWatermark - 1 and the last offset', () => {
      const batch = new Batch(topic, 0n, { partition: 0, highWatermark: 100n, messages: [msg(3n), msg(4n)] });
      expect(batch.offsetLag()).toEqual(95n);
    });

    it('returns 0 when the batch is empty', () => {
      const batch = new Batch(topic, 0n, { partition: 0, highWatermark: 100n, messages: [] });
      expect(batch.offsetLag()).toEqual(0n);
    });
  });

  describe('#offsetLagLow', () => {
    it('returns the difference between highWatermark - 1 and the first offset', () => {
      const batch = new Batch(topic, 0n, { partition: 0, highWatermark: 100n, messages: [msg(3n), msg(4n)] });
      expect(batch.offsetLagLow()).toEqual(96n);
    });

    it('returns 0 when the batch is empty', () => {
      const batch = new Batch(topic, 0n, { partition: 0, highWatermark: 100n, messages: [] });
      expect(batch.offsetLagLow()).toEqual(0n);
    });
  });

  describe('#isEmptyControlRecord', () => {
    it('returns false for regular batches', () => {
      const batch = new Batch(topic, 0n, { partition: 0, highWatermark: 100n, messages: [msg(3n), msg(4n)] });
      expect(batch.isEmptyControlRecord()).toEqual(false);
    });

    it('returns false for regular empty batches', () => {
      const batch = new Batch(topic, 0n, { partition: 0, highWatermark: 100n, messages: [] });
      expect(batch.isEmptyControlRecord()).toEqual(false);
    });

    it('returns false if there is a control record but some messages are available', () => {
      const batch = new Batch(topic, 0n, {
        partition: 0,
        highWatermark: 100n,
        messages: [msg(3n), msg(4n), msg(5n, { isControlRecord: true })],
      });
      expect(batch.isEmptyControlRecord()).toEqual(false);
    });

    it('returns true if the batch only contains a control record', () => {
      const batch = new Batch(topic, 0n, {
        partition: 0,
        highWatermark: 100n,
        messages: [msg(5n, { isControlRecord: true })],
      });
      expect(batch.isEmptyControlRecord()).toEqual(true);
    });
  });
});
