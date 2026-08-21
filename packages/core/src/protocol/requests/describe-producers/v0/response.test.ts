import { describe, expect, it } from 'vitest';
import responseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { KafkaAggregateError } from '../../../../errors';
import { describeProducersResponseV0 } from './response';

describe('protocol/requests/describe-producers/v0/response', () => {
  it('decodes active producer state with bigint protocol values', async () => {
    const data = await describeProducersResponseV0.decode(Buffer.from(responseFixture.data));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 7,
      topics: [
        {
          topic: 'orders',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              errorMessage: null,
              activeProducers: [
                {
                  producerId: 42n,
                  producerEpoch: 3,
                  lastSequence: 9,
                  lastTimestamp: 1000n,
                  coordinatorEpoch: 4,
                  currentTransactionStartOffset: -1n,
                },
              ],
            },
          ],
        },
      ],
    });
    await expect(describeProducersResponseV0.parse(data)).resolves.toEqual(data);
  });

  it('rejects partition errors', async () => {
    await expect(
      describeProducersResponseV0.parse({
        throttleTime: 0,
        clientSideThrottleTime: 0,
        topics: [
          {
            topic: 'orders',
            partitions: [{ partition: 0, errorCode: 3, errorMessage: 'unknown', activeProducers: [] }],
          },
        ],
      }),
    ).rejects.toBeInstanceOf(KafkaAggregateError);
  });
});
