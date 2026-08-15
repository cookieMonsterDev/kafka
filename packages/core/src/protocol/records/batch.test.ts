import { describe, expect, it } from 'vitest'
import { COMPRESSION_TYPES } from '../compression/index.js'
import { Decoder } from '../decoder.js'
import { TIMESTAMP_TYPES } from '../enums/timestamp-types.js'
import { decodeRecordBatch, encodeRecordBatch } from './batch.js'
import { encodeRecord } from './record.js'

// Captured from kafkajs's recordBatch/fixtures/v0_recordbatch.json — one gzip-compressed record.
const V0_RECORD_BATCH_FIXTURE = Buffer.from([
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 113, 0, 0, 0, 0, 2, 228, 195, 36, 165, 0, 9, 0, 0, 0, 0, 0, 0, 1, 115, 237, 245, 255,
  167, 0, 0, 1, 115, 237, 245, 255, 201, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 31, 139,
  8, 0, 0, 0, 0, 0, 0, 19, 11, 99, 96, 96, 224, 202, 78, 173, 212, 53, 112, 40, 75, 204, 41, 77, 213, 53, 208, 53, 50,
  48, 50, 208, 53, 176, 208, 53, 52, 9, 49, 52, 183, 50, 52, 182, 50, 176, 208, 51, 179, 48, 138, 98, 0, 0, 181, 227,
  241, 167, 44, 0, 0, 0,
])

describe('protocol/records/batch', () => {
  it('decodes a known-good gzip-compressed fixture', async () => {
    const decoded = await decodeRecordBatch(new Decoder(V0_RECORD_BATCH_FIXTURE))

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
    })
    expect(decoded.records).toHaveLength(1)
  })

  it('round-trips an uncompressed batch with multiple records', async () => {
    const records = [
      encodeRecord({ offsetDelta: 0, key: 'k0', value: 'v0' }),
      encodeRecord({ offsetDelta: 1, key: 'k1', value: 'v1' }),
      encodeRecord({ offsetDelta: 2, key: 'k2', value: 'v2' }),
    ]

    const encoded = await encodeRecordBatch({
      firstOffset: 100n,
      lastOffsetDelta: 2,
      records,
    })

    const fetchDecoder = new Decoder(encoded.buffer)
    const decoded = await decodeRecordBatch(fetchDecoder)

    expect(decoded.firstOffset).toBe(100n)
    expect(decoded.records).toHaveLength(3)
    expect(decoded.records.map((r) => r.key?.toString())).toEqual(['k0', 'k1', 'k2'])
    expect(decoded.records.map((r) => r.value?.toString())).toEqual(['v0', 'v1', 'v2'])
    expect(decoded.records.map((r) => r.offset)).toEqual([100n, 101n, 102n])
  })

  it('round-trips a gzip-compressed batch', async () => {
    const records = [encodeRecord({ key: 'k', value: 'v'.repeat(100) })]
    const encoded = await encodeRecordBatch({ compression: COMPRESSION_TYPES.GZIP, records })

    const decoded = await decodeRecordBatch(new Decoder(encoded.buffer))
    expect(decoded.records).toHaveLength(1)
    expect(decoded.records[0]?.value?.toString()).toBe('v'.repeat(100))
  })

  it('round-trips a zstd-compressed batch', async () => {
    const records = [encodeRecord({ key: 'k', value: 'v'.repeat(100) })]
    const encoded = await encodeRecordBatch({ compression: COMPRESSION_TYPES.ZSTD, records })

    const decoded = await decodeRecordBatch(new Decoder(encoded.buffer))
    expect(decoded.records).toHaveLength(1)
    expect(decoded.records[0]?.value?.toString()).toBe('v'.repeat(100))
  })

  it('marks the batch transactional when requested', async () => {
    const encoded = await encodeRecordBatch({ transactional: true, records: [encodeRecord({ value: 'v' })] })
    const decoded = await decodeRecordBatch(new Decoder(encoded.buffer))
    expect(decoded.inTransaction).toBe(true)
  })

  it('throws KafkaJSPartialMessageError when the batch is truncated', async () => {
    const encoded = await encodeRecordBatch({ records: [encodeRecord({ value: 'v' })] })
    const truncated = encoded.buffer.subarray(0, encoded.buffer.length - 5)
    await expect(decodeRecordBatch(new Decoder(truncated))).rejects.toThrow(/partial record batch/)
  })
})
