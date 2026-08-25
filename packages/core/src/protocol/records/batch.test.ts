import { describe, expect, it } from 'vitest';
import { KafkaCorruptRecordError } from '../../errors';
import { COMPRESSION_TYPES } from '../compression/index';
import { Decoder } from '../decoder';
import { TIMESTAMP_TYPES } from '../enums/timestamp-types';
import { decodeRecordBatch, encodeRecordBatch } from './batch';
import { encodeRecord } from './record';

// One gzip-compressed record batch.
const V0_RECORD_BATCH_FIXTURE = Buffer.from([
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 113, 0, 0, 0, 0, 2, 228, 195, 36, 165, 0, 9, 0, 0, 0, 0, 0, 0, 1, 115, 237, 245, 255,
  167, 0, 0, 1, 115, 237, 245, 255, 201, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 31, 139,
  8, 0, 0, 0, 0, 0, 0, 19, 11, 99, 96, 96, 224, 202, 78, 173, 212, 53, 112, 40, 75, 204, 41, 77, 213, 53, 208, 53, 50,
  48, 50, 208, 53, 176, 208, 53, 52, 9, 49, 52, 183, 50, 52, 182, 50, 176, 208, 51, 179, 48, 138, 98, 0, 0, 181, 227,
  241, 167, 44, 0, 0, 0,
]);

describe('protocol/records/batch', () => {
  it('decodes a known-good gzip-compressed fixture', async () => {
    const decoded = await decodeRecordBatch(new Decoder(V0_RECORD_BATCH_FIXTURE));

    expect(decoded).toMatchObject({
      firstOffset: 0n,
      firstTimestamp: 1597425188775n,
      partitionLeaderEpoch: 0,
      inTransaction: false,
      isControlBatch: false,
      lastOffsetDelta: 0,
      producerId: -1n,
      producerEpoch: 0,
      firstSequence: 0,
      maxTimestamp: 1597425188809n,
      timestampType: TIMESTAMP_TYPES.LOG_APPEND_TIME,
    });
    expect(decoded.records).toHaveLength(1);
  });

  it('round-trips an uncompressed batch with multiple records', async () => {
    const records = [
      encodeRecord({ offsetDelta: 0, key: 'k0', value: 'v0' }),
      encodeRecord({ offsetDelta: 1, key: 'k1', value: 'v1' }),
      encodeRecord({ offsetDelta: 2, key: 'k2', value: 'v2' }),
    ];

    const encoded = await encodeRecordBatch({
      firstOffset: 100n,
      lastOffsetDelta: 2,
      records,
    });

    const fetchDecoder = new Decoder(encoded.buffer);
    const decoded = await decodeRecordBatch(fetchDecoder);

    expect(decoded.firstOffset).toBe(100n);
    expect(decoded.records).toHaveLength(3);
    expect(decoded.records.map((r) => r.key?.toString())).toEqual(['k0', 'k1', 'k2']);
    expect(decoded.records.map((r) => r.value?.toString())).toEqual(['v0', 'v1', 'v2']);
    expect(decoded.records.map((r) => r.offset)).toEqual([100n, 101n, 102n]);
  });

  it('round-trips a gzip-compressed batch', async () => {
    const records = [encodeRecord({ key: 'k', value: 'v'.repeat(100) })];
    const encoded = await encodeRecordBatch({ compression: COMPRESSION_TYPES.GZIP, records });

    const decoded = await decodeRecordBatch(new Decoder(encoded.buffer));
    expect(decoded.records).toHaveLength(1);
    expect(decoded.records[0]?.value?.toString()).toBe('v'.repeat(100));
  });

  it('round-trips an lz4-compressed batch', async () => {
    const records = [encodeRecord({ key: 'k', value: 'v'.repeat(100) })];
    const encoded = await encodeRecordBatch({ compression: COMPRESSION_TYPES.LZ4, records });

    const decoded = await decodeRecordBatch(new Decoder(encoded.buffer));
    expect(decoded.records).toHaveLength(1);
    expect(decoded.records[0]?.value?.toString()).toBe('v'.repeat(100));
  });

  it('round-trips a zstd-compressed batch', async () => {
    const records = [encodeRecord({ key: 'k', value: 'v'.repeat(100) })];
    const encoded = await encodeRecordBatch({ compression: COMPRESSION_TYPES.ZSTD, records });

    const decoded = await decodeRecordBatch(new Decoder(encoded.buffer));
    expect(decoded.records).toHaveLength(1);
    expect(decoded.records[0]?.value?.toString()).toBe('v'.repeat(100));
  });

  it('marks the batch transactional when requested', async () => {
    const encoded = await encodeRecordBatch({ transactional: true, records: [encodeRecord({ value: 'v' })] });
    const decoded = await decodeRecordBatch(new Decoder(encoded.buffer));
    expect(decoded.inTransaction).toBe(true);
  });

  it('throws KafkaPartialMessageError when the batch is truncated', async () => {
    const encoded = await encodeRecordBatch({ records: [encodeRecord({ value: 'v' })] });
    const truncated = encoded.buffer.subarray(0, encoded.buffer.length - 5);
    await expect(decodeRecordBatch(new Decoder(truncated))).rejects.toThrow(/partial record batch/);
  });

  it('in-place writeRecords matches pre-encoded records byte-for-byte', async () => {
    const options = {
      firstOffset: 100n,
      firstTimestamp: 1_500_000_000_000,
      maxTimestamp: 1_500_000_000_100,
      lastOffsetDelta: 2,
      producerId: 7n,
      producerEpoch: 1,
      firstSequence: 3,
    };
    const records = [
      encodeRecord({ offsetDelta: 0, key: 'k0', value: 'v0' }),
      encodeRecord({ offsetDelta: 1, key: 'k1', value: 'v1' }),
      encodeRecord({ offsetDelta: 2, key: 'k2', value: 'v2' }),
    ];

    const copied = await encodeRecordBatch({ ...options, records });
    const inplace = await encodeRecordBatch({
      ...options,
      recordCount: 3,
      writeRecords: (encoder) => {
        encodeRecord({ offsetDelta: 0, key: 'k0', value: 'v0' }, encoder);
        encodeRecord({ offsetDelta: 1, key: 'k1', value: 'v1' }, encoder);
        encodeRecord({ offsetDelta: 2, key: 'k2', value: 'v2' }, encoder);
      },
    });

    expect(inplace.buffer).toEqual(copied.buffer);
  });

  it('compressionLevel threads through to the codec: gzip level 1 vs 9 change the encoded size', async () => {
    const records = [encodeRecord({ key: 'k', value: 'kafka-kafka-kafka-'.repeat(200) })];

    const fast = await encodeRecordBatch({ compression: COMPRESSION_TYPES.GZIP, compressionLevel: 1, records });
    const best = await encodeRecordBatch({ compression: COMPRESSION_TYPES.GZIP, compressionLevel: 9, records });

    expect(fast.buffer.length).toBeGreaterThan(best.buffer.length);

    const decodedFast = await decodeRecordBatch(new Decoder(fast.buffer));
    const decodedBest = await decodeRecordBatch(new Decoder(best.buffer));
    expect(decodedFast.records[0]?.value?.toString()).toBe('kafka-kafka-kafka-'.repeat(200));
    expect(decodedBest.records[0]?.value?.toString()).toBe('kafka-kafka-kafka-'.repeat(200));
  });

  describe('checkCrcs', () => {
    it('defaults to true and rejects a batch whose CRC does not match its bytes', async () => {
      const encoded = await encodeRecordBatch({ records: [encodeRecord({ key: 'k', value: 'v' })] });
      const corrupted = Buffer.from(encoded.buffer);
      // Flip a byte inside the record payload (after the fixed header) without touching the CRC field.
      const flipIndex = corrupted.length - 1;
      corrupted[flipIndex] = (corrupted[flipIndex] as number) ^ 0xff;

      await expect(decodeRecordBatch(new Decoder(corrupted))).rejects.toBeInstanceOf(KafkaCorruptRecordError);
      await expect(decodeRecordBatch(new Decoder(corrupted))).rejects.toThrow(/CRC mismatch/);
    });

    it('checkCrcs: false skips the check and decodes the corrupted batch anyway', async () => {
      const encoded = await encodeRecordBatch({ records: [encodeRecord({ key: 'k', value: 'v' })] });
      const corrupted = Buffer.from(encoded.buffer);
      const flipIndex = corrupted.length - 1;
      corrupted[flipIndex] = (corrupted[flipIndex] as number) ^ 0xff;

      const decoded = await decodeRecordBatch(new Decoder(corrupted), { checkCrcs: false });
      expect(decoded.records).toHaveLength(1);
    });

    it('checkCrcs: true (explicit) still accepts a valid batch', async () => {
      const encoded = await encodeRecordBatch({ records: [encodeRecord({ key: 'k', value: 'v' })] });
      const decoded = await decodeRecordBatch(new Decoder(encoded.buffer), { checkCrcs: true });
      expect(decoded.records).toHaveLength(1);
    });
  });
});
