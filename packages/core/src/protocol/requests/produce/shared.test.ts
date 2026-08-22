import { describe, expect, it } from 'vitest';
import { Decoder } from '../../decoder';
import { decodeRecordBatch } from '../../records/batch';
import { createProduceRequest } from './shared';

async function decodeFirstBatch(messages: { value: string; timestamp?: number }[]) {
  const encoded = await createProduceRequest(3, {
    acks: 1,
    timeout: 1000,
    topicData: [{ topic: 't', partitions: [{ partition: 0, messages }] }],
  }).encode();

  const decoder = new Decoder(encoded.buffer);
  decoder.readString(); // transactionalId
  decoder.readInt16(); // acks
  decoder.readInt32(); // timeout
  decoder.readInt32(); // topic count
  decoder.readString(); // topic
  decoder.readInt32(); // partition count
  decoder.readInt32(); // partition
  const recordSet = decoder.readBytes();
  if (recordSet === null) throw new Error('expected a record set');
  return decodeRecordBatch(new Decoder(recordSet));
}

describe('protocol/requests/produce/shared', () => {
  it('sets firstTimestamp and maxTimestamp from a single min/max pass', async () => {
    const batch = await decodeFirstBatch([
      { value: 'a', timestamp: 500 },
      { value: 'b', timestamp: 100 },
      { value: 'c', timestamp: 300 },
    ]);
    expect(batch.firstTimestamp).toBe(100n);
    expect(batch.maxTimestamp).toBe(500n);
    expect(batch.records.map((r) => r.timestamp)).toEqual([500n, 100n, 300n]);
  });

  it('uses Date.now() when no message timestamps are set', async () => {
    const before = Date.now();
    const batch = await decodeFirstBatch([{ value: 'a' }, { value: 'b' }]);
    const after = Date.now();
    expect(batch.firstTimestamp).toBe(batch.maxTimestamp);
    expect(batch.firstTimestamp).toBeGreaterThanOrEqual(BigInt(before));
    expect(batch.firstTimestamp).toBeLessThanOrEqual(BigInt(after));
  });

  it('ignores missing timestamps when computing min/max', async () => {
    const batch = await decodeFirstBatch([
      { value: 'a', timestamp: 200 },
      { value: 'b' },
      { value: 'c', timestamp: 50 },
    ]);
    expect(batch.firstTimestamp).toBe(50n);
    expect(batch.maxTimestamp).toBe(200n);
  });
});
