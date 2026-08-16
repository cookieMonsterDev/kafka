import { describe, expect, it } from 'vitest';
import v4ResponseFixture from '../fixtures/v4-response.json' with { type: 'json' };
import { fetchResponseV4 } from './response';

const batchContext = {
  firstOffset: expect.any(BigInt),
  firstSequence: expect.any(Number),
  firstTimestamp: expect.any(BigInt),
  inTransaction: expect.any(Boolean),
  isControlBatch: expect.any(Boolean),
  lastOffsetDelta: expect.any(Number),
  magicByte: expect.any(Number),
  maxTimestamp: expect.any(BigInt),
  partitionLeaderEpoch: expect.any(Number),
  producerEpoch: expect.any(Number),
  producerId: expect.any(BigInt),
  timestampType: expect.any(Number),
};

describe('protocol/requests/fetch/v4/response', () => {
  it('decodes a real fixture, including uncompressed record batches', async () => {
    const data = await fetchResponseV4.decode(Buffer.from(v4ResponseFixture.data));

    expect(data).toEqual({
      throttleTime: 0,
      responses: [
        {
          topicName: 'test-topic-ab4d54774dcadc395a7f',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              highWatermark: 3n,
              lastStableOffset: 3n,
              abortedTransactions: [],
              messages: [0, 1, 2].map((i) => ({
                offset: BigInt(i),
                magicByte: 2,
                attributes: 0,
                batchContext,
                timestamp: 1509827900073n,
                headers: { [`header-key-${i}`]: Buffer.from(`header-value-${i}`) },
                key: Buffer.from(`key-${i}`),
                value: Buffer.from(`some-value-${i}`),
                isControlRecord: false,
              })),
            },
          ],
        },
      ],
    });

    await expect(fetchResponseV4.parse(data)).resolves.toBeTruthy();
  });
});
