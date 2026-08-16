import { describe, expect, it } from 'vitest';
import v5ResponseFixture from '../fixtures/v5-response.json' with { type: 'json' };
import { fetchResponseV5 } from './response';

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

describe('protocol/requests/fetch/v5/response', () => {
  it('decodes a real fixture, including logStartOffset', async () => {
    const data = await fetchResponseV5.decode(Buffer.from(v5ResponseFixture.data));

    expect(data).toEqual({
      throttleTime: 0,
      responses: [
        {
          topicName: 'test-topic-c935d678835de2c9c79e-2064-677041b7-df54-4d4d-a53a-b9133d2fdc8c',
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

    await expect(fetchResponseV5.parse(data)).resolves.toBeTruthy();
  });
});
