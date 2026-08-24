import { describe, expect, it } from 'vitest';
import v11ResponseFixture from '../fixtures/v11-response.json' with { type: 'json' };
import { fetchResponseV11 } from './response';

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

describe('protocol/requests/fetch/v11/response', () => {
  it('decodes a real fixture, including preferredReadReplica', async () => {
    const data = await fetchResponseV11.decode(Buffer.from(v11ResponseFixture.data));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      sessionId: 0,
      responses: [
        {
          topicName: 'test-topic-2077b9d2b36c4082e594-4020-b5a52b27-56df-4b87-800d-82c1cf26317d',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              highWatermark: 3n,
              lastStableOffset: 3n,
              logStartOffset: 0n,
              abortedTransactions: [],
              preferredReadReplica: 0,
              currentLeader: null,
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
      nodeEndpoints: [],
    });

    await expect(fetchResponseV11.parse(data)).resolves.toBeTruthy();
  });
});
