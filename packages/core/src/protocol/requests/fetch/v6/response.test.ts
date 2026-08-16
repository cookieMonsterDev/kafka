import { describe, expect, it } from 'vitest';
import v6ResponseFixture from '../fixtures/v6-response.json' with { type: 'json' };
import { fetchResponseV6 } from './response';

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

describe('protocol/requests/fetch/v6/response', () => {
  it('decodes a real fixture (wire shape identical to v5)', async () => {
    const data = await fetchResponseV6.decode(Buffer.from(v6ResponseFixture.data));

    expect(data).toEqual({
      throttleTime: 0,
      responses: [
        {
          topicName: 'test-topic-07eae0edd6400fe2733a-3088-330080bb-97f1-4a09-89e1-f0fe5c137ab2',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              highWatermark: 3n,
              lastStableOffset: 3n,
              logStartOffset: 0n,
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

    await expect(fetchResponseV6.parse(data)).resolves.toBeTruthy();
  });
});
