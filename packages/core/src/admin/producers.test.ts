import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import type { Logger } from '../loggers/index';
import { createProducersApi } from './producers';
import { withSend } from '../broker/test-send';
import { fakeRunCluster } from '../cluster/test-run';

function response(topic: string, partition: number, producerId: bigint) {
  return {
    topics: [
      {
        topic,
        partitions: [
          {
            partition,
            errorCode: 0,
            errorMessage: null,
            activeProducers: [
              {
                producerId,
                producerEpoch: 1,
                lastSequence: 2,
                lastTimestamp: 3n,
                coordinatorEpoch: 4,
                currentTransactionStartOffset: -1n,
              },
            ],
          },
        ],
      },
    ],
  };
}

describe('admin.producers', () => {
  it('fans requests out to partition leaders and flattens their state', async () => {
    const brokerOne = withSend({ describeProducers: vi.fn().mockResolvedValue(response('orders', 0, 10n)) });
    const brokerTwo = withSend({ describeProducers: vi.fn().mockResolvedValue(response('orders', 1, 11n)) });
    const cluster = fakeRunCluster(
      {},
      {
        metadata: vi.fn().mockResolvedValue({}),
        findLeaderForPartitions: vi.fn().mockReturnValue({ 1: [0], 2: [1] }),
        findBroker: vi.fn(({ nodeId }: { nodeId: string }) => Promise.resolve(nodeId === '1' ? brokerOne : brokerTwo)),
      },
    );
    const api = createProducersApi({
      cluster: cluster as unknown as Cluster,
      logger: {} as Logger,
      rootLogger: {} as Logger,
    });

    await expect(
      api.describeProducers({ topicPartitions: [{ topic: 'orders', partitions: [0, 1] }] }),
    ).resolves.toEqual([
      {
        topic: 'orders',
        partition: 0,
        activeProducers: [expect.objectContaining({ producerId: 10n, currentTransactionStartOffset: null })],
      },
      {
        topic: 'orders',
        partition: 1,
        activeProducers: [expect.objectContaining({ producerId: 11n, currentTransactionStartOffset: null })],
      },
    ]);
    expect(cluster.run).toHaveBeenCalledWith(
      expect.any(Function),
      expect.any(Function),
      expect.objectContaining({ retryRetriable: true }),
    );
    expect(brokerOne.describeProducers).toHaveBeenCalledWith({
      topics: [{ topic: 'orders', partitions: [0] }],
    });
    expect(brokerTwo.describeProducers).toHaveBeenCalledWith({
      topics: [{ topic: 'orders', partitions: [1] }],
    });
  });

  it('rejects invalid topic partitions', async () => {
    const api = createProducersApi({
      cluster: {} as Cluster,
      logger: {} as Logger,
      rootLogger: {} as Logger,
    });

    await expect(api.describeProducers({ topicPartitions: [] })).rejects.toThrow('Invalid topicPartitions array');
    await expect(api.describeProducers({ topicPartitions: [{ topic: 'orders', partitions: [-1] }] })).rejects.toThrow(
      'Invalid partitions array',
    );
  });
});
