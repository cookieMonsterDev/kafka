import { describe, expect, it } from 'vitest';
import { KafkaOffsetOutOfRange } from '../../../../errors';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import v0ResponseGzipFixture from '../fixtures/v0-response-gzip.json' with { type: 'json' };
import { fetchResponseV0 } from './response';

const messageSetRecord = (i: number, crc: number) => ({
  offset: BigInt(i),
  size: 31,
  crc,
  magicByte: 0,
  attributes: 0,
  timestamp: 0n,
  key: Buffer.from(`key-${i}`),
  value: Buffer.from(`some-value-${i}`),
  headers: {},
  isControlRecord: false,
  batchContext: expect.objectContaining({ magicByte: 0, firstOffset: BigInt(i), producerId: -1n }),
});

describe('protocol/requests/fetch/v0/response', () => {
  it('decodes a real fixture, including uncompressed MessageSet records', async () => {
    const data = await fetchResponseV0.decode(Buffer.from(v0ResponseFixture.data));
    expect(data).toEqual({
      responses: [
        {
          topicName: 'test-topic-79b94d9dcfd65e1283a9',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              highWatermark: 1n,
              messages: [messageSetRecord(0, 120234579)],
            },
          ],
        },
      ],
    });
    await expect(fetchResponseV0.parse(data)).resolves.toBeTruthy();
  });

  it('decodes a gzip MessageSet fixture', async () => {
    const data = await fetchResponseV0.decode(Buffer.from(v0ResponseGzipFixture.data));
    expect(data.responses[0]?.partitions[0]?.messages).toEqual([
      messageSetRecord(0, 120234579),
      messageSetRecord(1, -141862522),
      messageSetRecord(2, 1025004472),
    ]);
    expect(data.responses[0]?.topicName).toBe('test-topic-be3cb8c367c9d903933f');
    expect(data.responses[0]?.partitions[0]?.highWatermark).toBe(3n);
  });

  it('throws KafkaOffsetOutOfRange when a partition reports OFFSET_OUT_OF_RANGE', async () => {
    await expect(
      fetchResponseV0.parse({
        responses: [
          {
            topicName: 't',
            partitions: [{ partition: 0, errorCode: 1, highWatermark: 1n, messages: [] }],
          },
        ],
      }),
    ).rejects.toBeInstanceOf(KafkaOffsetOutOfRange);
  });
});
