import { describe, expect, it, vi } from 'vitest';
import { SHARE_ACKNOWLEDGE_TYPE } from './acknowledge-types';
import type { ShareGroup } from './share-group';
import { ShareRunner } from './share-runner';

const TOPIC_ID = Buffer.alloc(16, 9);

const BATCH_CONTEXT = {
  firstOffset: 10n,
  firstTimestamp: 1n,
  partitionLeaderEpoch: 0,
  inTransaction: false,
  isControlBatch: false,
  lastOffsetDelta: 0,
  producerId: -1n,
  producerEpoch: -1,
  firstSequence: -1,
  maxTimestamp: 1n,
  timestampType: 0,
  magicByte: 2,
} as const;

function createShareGroup(overrides: Record<string, unknown> = {}) {
  const shareFetch = vi.fn().mockResolvedValue({
    errorCode: 0,
    responses: [
      {
        topicId: TOPIC_ID,
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0,
            records: [
              {
                magicByte: 2,
                attributes: 0,
                timestamp: 1n,
                offset: 10n,
                key: Buffer.from('k'),
                value: Buffer.from('v'),
                headers: {},
                isControlRecord: false,
                batchContext: BATCH_CONTEXT,
              },
            ],
            acquiredRecords: [{ firstOffset: 10n, lastOffset: 10n, deliveryCount: 1 }],
          },
        ],
      },
    ],
  });
  const shareAcknowledge = vi.fn().mockResolvedValue({ errorCode: 0, responses: [] });
  const findBroker = vi.fn().mockResolvedValue({ shareFetch, shareAcknowledge });
  const shareGroup = {
    groupId: 'share-1',
    memberId: 'member-1',
    heartbeatIntervalMs: 50,
    cluster: {
      refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
      findTopicId: vi.fn().mockReturnValue(TOPIC_ID),
      findBroker,
    },
    connect: vi.fn().mockResolvedValue(undefined),
    joinAndSync: vi.fn().mockResolvedValue(undefined),
    leave: vi.fn().mockResolvedValue(undefined),
    disconnect: vi.fn().mockResolvedValue(undefined),
    heartbeat: vi.fn().mockResolvedValue(undefined),
    getNodeIds: vi.fn().mockReturnValue(['1']),
    filterPartitionsByNode: vi.fn().mockReturnValue([{ topic: 'events', partitions: [0] }]),
    assigned: vi.fn().mockReturnValue([{ topic: 'events', partitions: [0] }]),
    recoverFromFetch: vi.fn(),
    ...overrides,
  } as unknown as ShareGroup;

  return { shareGroup, shareFetch, shareAcknowledge };
}

describe('share-consumer/share-runner', () => {
  it('fetches acquired records, accepts them, and closes the share session on stop', async () => {
    const leave = vi.fn().mockResolvedValue(undefined);
    const { shareGroup, shareFetch, shareAcknowledge } = createShareGroup({ leave });
    const handled: bigint[] = [];
    const runner = new ShareRunner({
      shareGroup,
      heartbeatInterval: 50,
      maxWaitTimeInMs: 10,
      retry: { retries: 0 },
      eachMessage: async ({ message }) => {
        handled.push(message.offset);
        runner.shuttingDown = true;
      },
      onCrash: vi.fn(),
    });

    await runner.start();
    await vi.waitFor(() => expect(handled).toEqual([10n]));
    await runner.stop();

    expect(handled).toEqual([10n]);
    expect(shareFetch).toHaveBeenCalledWith(
      expect.objectContaining({
        shareAcquireMode: 0,
        isRenewAck: false,
      }),
    );
    expect(shareAcknowledge).toHaveBeenCalledWith(
      expect.objectContaining({
        groupId: 'share-1',
        memberId: 'member-1',
        shareSessionEpoch: -1,
      }),
    );
    expect(leave).toHaveBeenCalled();
  });

  it('releases acquired records when the handler fails', async () => {
    const { shareGroup, shareAcknowledge } = createShareGroup();
    const onCrash = vi.fn();
    const runner = new ShareRunner({
      shareGroup,
      heartbeatInterval: 50,
      maxWaitTimeInMs: 10,
      retry: { retries: 0 },
      eachMessage: async () => {
        throw new Error('boom');
      },
      onCrash,
    });

    await runner.start();
    await vi.waitFor(() => expect(onCrash).toHaveBeenCalled());
    runner.shuttingDown = true;
    await runner.stop();

    const closeCall = shareAcknowledge.mock.calls[0]?.[0] as {
      topics: { partitions: { acknowledgementBatches: { acknowledgeTypes: number[] }[] }[] }[];
    };
    expect(closeCall.topics[0]?.partitions[0]?.acknowledgementBatches[0]?.acknowledgeTypes).toEqual([
      SHARE_ACKNOWLEDGE_TYPE.RELEASE,
    ]);
  });

  it('sends ShareFetch v2 record-limit mode and marks renew acknowledgements', async () => {
    const { shareGroup, shareFetch } = createShareGroup();
    const runner = new ShareRunner({
      shareGroup,
      heartbeatInterval: 50,
      maxWaitTimeInMs: 10,
      shareAcquireMode: 1,
      retry: { retries: 0 },
      eachMessage: async () => {
        runner.shuttingDown = true;
      },
      onCrash: vi.fn(),
    });

    await runner.start();
    await vi.waitFor(() => expect(shareFetch).toHaveBeenCalled());
    await runner.stop();

    expect(shareFetch).toHaveBeenCalledWith(
      expect.objectContaining({
        shareAcquireMode: 1,
        isRenewAck: false,
      }),
    );
  });
});
