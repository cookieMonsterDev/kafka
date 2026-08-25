import { describe, expect, it, vi } from 'vitest';
import { InstrumentationEventEmitter } from '../instrumentation/emitter';
import type { InstrumentationEvent } from '../instrumentation/event';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createErrorFromCode, ERROR_CODES } from '../protocol/error-codes';
import { SHARE_ACKNOWLEDGE_TYPE } from './acknowledge-types';
import { ACKNOWLEDGE, FETCH, FETCH_START, type ShareAcknowledgePayload } from './instrumentation-events';
import type { ShareGroup } from './share-group';
import { ShareRunner } from './share-runner';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });
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
    heartbeatDue: vi.fn().mockReturnValue(false),
    getNodeIds: vi.fn().mockReturnValue(['1']),
    filterPartitionsByNode: vi.fn().mockReturnValue([{ topic: 'events', partitions: [0] }]),
    assigned: vi.fn().mockReturnValue([{ topic: 'events', partitions: [0] }]),
    hasAssignment: vi.fn().mockReturnValue(true),
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
      logger: silentLogger,
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
      logger: silentLogger,
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
      logger: silentLogger,
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

  it('exposes eachBatch and auto-acks acquired records', async () => {
    const { shareGroup, shareAcknowledge } = createShareGroup();
    const seen: string[] = [];
    const runner = new ShareRunner({
      logger: silentLogger,
      shareGroup,
      heartbeatInterval: 50,
      maxWaitTimeInMs: 10,
      retry: { retries: 0 },
      eachBatch: async ({ batch, acknowledge }) => {
        seen.push(`${batch.topic}:${batch.partition}`);
        acknowledge();
        runner.shuttingDown = true;
      },
      onCrash: vi.fn(),
    });

    await runner.start();
    await vi.waitFor(() => expect(seen).toEqual(['events:0']));
    await runner.stop();

    const closeCall = shareAcknowledge.mock.calls[0]?.[0] as {
      topics: { partitions: { acknowledgementBatches: { acknowledgeTypes: number[] }[] }[] }[];
    };
    expect(closeCall.topics[0]?.partitions[0]?.acknowledgementBatches[0]?.acknowledgeTypes).toEqual([
      SHARE_ACKNOWLEDGE_TYPE.ACCEPT,
    ]);
  });

  it('fetches from each assigned node in parallel', async () => {
    const shareFetchA = vi.fn().mockImplementation(async () => {
      await new Promise((resolve) => setTimeout(resolve, 40));
      return { errorCode: 0, responses: [] };
    });
    const shareFetchB = vi.fn().mockImplementation(async () => {
      await new Promise((resolve) => setTimeout(resolve, 40));
      return { errorCode: 0, responses: [] };
    });
    const shareAcknowledge = vi.fn().mockResolvedValue({ errorCode: 0, responses: [] });
    const findBroker = vi.fn(async ({ nodeId }: { nodeId: string }) => ({
      shareFetch: nodeId === '1' ? shareFetchA : shareFetchB,
      shareAcknowledge,
    }));
    const { shareGroup } = createShareGroup({
      getNodeIds: vi.fn().mockReturnValue(['1', '2']),
      filterPartitionsByNode: vi.fn((nodeId: string) => [{ topic: 'events', partitions: nodeId === '1' ? [0] : [1] }]),
      assigned: vi.fn().mockReturnValue([{ topic: 'events', partitions: [0, 1] }]),
      cluster: {
        refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
        findTopicId: vi.fn().mockReturnValue(TOPIC_ID),
        findBroker,
      },
    });
    const runner = new ShareRunner({
      logger: silentLogger,
      shareGroup,
      heartbeatInterval: 50,
      maxWaitTimeInMs: 10,
      retry: { retries: 0 },
      eachMessage: async () => {
        runner.shuttingDown = true;
      },
      onCrash: vi.fn(),
    });

    const started = Date.now();
    await runner.start();
    await vi.waitFor(() => expect(shareFetchA).toHaveBeenCalled());
    await vi.waitFor(() => expect(shareFetchB).toHaveBeenCalled());
    const elapsed = Date.now() - started;
    await runner.stop();
    expect(elapsed).toBeLessThan(80);
  });

  it('forgets a partition dropped from the assignment on the next share fetch for that node', async () => {
    const shareFetch = vi.fn().mockResolvedValue({ errorCode: 0, responses: [] });
    const shareAcknowledge = vi.fn().mockResolvedValue({ errorCode: 0, responses: [] });
    const findBroker = vi.fn().mockResolvedValue({ shareFetch, shareAcknowledge });
    let assignedPartitions = [0, 1];
    const { shareGroup } = createShareGroup({
      filterPartitionsByNode: vi.fn(() => [{ topic: 'events', partitions: assignedPartitions }]),
      cluster: {
        refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
        findTopicId: vi.fn().mockReturnValue(TOPIC_ID),
        findBroker,
      },
    });

    const runner = new ShareRunner({
      logger: silentLogger,
      shareGroup,
      heartbeatInterval: 50,
      maxWaitTimeInMs: 10,
      retry: { retries: 0 },
      onCrash: vi.fn(),
    });

    type ShareFetchCallArgs = { forgottenTopics: { topicId: Buffer; partitions: number[] }[] };
    const findForgetCall = () =>
      (shareFetch.mock.calls as [ShareFetchCallArgs][]).find(([opts]) => opts.forgottenTopics.length > 0);

    await runner.start();
    await vi.waitFor(() => expect(shareFetch).toHaveBeenCalled());
    expect(findForgetCall()).toBeUndefined();

    assignedPartitions = [0];
    await vi.waitFor(() => expect(findForgetCall()).toBeDefined());
    runner.shuttingDown = true;
    await runner.stop();

    expect(findForgetCall()?.[0]?.forgottenTopics).toEqual([{ topicId: TOPIC_ID, partitions: [1] }]);
  });

  it('resets the share session epoch after the broker invalidates it, then retries fresh', async () => {
    const code = ERROR_CODES.find((entry) => entry.type === 'INVALID_SHARE_SESSION_EPOCH')!.code;
    const shareFetch = vi
      .fn()
      .mockResolvedValueOnce({ errorCode: 0, responses: [] })
      .mockRejectedValueOnce(createErrorFromCode(code))
      .mockResolvedValueOnce({ errorCode: 0, responses: [] });
    const shareAcknowledge = vi.fn().mockResolvedValue({ errorCode: 0, responses: [] });
    const findBroker = vi.fn().mockResolvedValue({ shareFetch, shareAcknowledge });
    const { shareGroup } = createShareGroup({
      cluster: {
        refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
        findTopicId: vi.fn().mockReturnValue(TOPIC_ID),
        findBroker,
      },
    });

    const onCrash = vi.fn();
    const runner = new ShareRunner({
      logger: silentLogger,
      shareGroup,
      heartbeatInterval: 50,
      maxWaitTimeInMs: 10,
      retry: { retries: 0 },
      onCrash,
    });

    await runner.start();
    // The mock's fourth call (unconfigured) returns `undefined`, which crashes the runner - a
    // deterministic stopping point once the three configured responses above have been used.
    await vi.waitFor(() => expect(onCrash).toHaveBeenCalled());
    runner.shuttingDown = true;
    await runner.stop();

    expect(shareFetch.mock.calls[0]?.[0]?.shareSessionEpoch).toBe(0);
    expect(shareFetch.mock.calls[1]?.[0]?.shareSessionEpoch).toBe(1);
    expect(shareFetch.mock.calls[2]?.[0]?.shareSessionEpoch).toBe(0);
  });

  it('emits FETCH_START and FETCH around each fetch round', async () => {
    const { shareGroup } = createShareGroup();
    const instrumentationEmitter = new InstrumentationEventEmitter();
    const fetchStarts: unknown[] = [];
    const fetches: unknown[] = [];
    instrumentationEmitter.addListener(FETCH_START, (event: InstrumentationEvent<unknown>) => {
      fetchStarts.push(event.payload);
    });
    instrumentationEmitter.addListener(FETCH, (event: InstrumentationEvent<unknown>) => {
      fetches.push(event.payload);
    });

    const runner = new ShareRunner({
      logger: silentLogger,
      shareGroup,
      heartbeatInterval: 50,
      maxWaitTimeInMs: 10,
      retry: { retries: 0 },
      instrumentationEmitter,
      eachMessage: async () => {
        runner.shuttingDown = true;
      },
      onCrash: vi.fn(),
    });

    await runner.start();
    await vi.waitFor(() => expect(fetches.length).toBeGreaterThan(0));
    await runner.stop();

    expect(fetchStarts[0]).toEqual({ nodeId: '1' });
    expect(fetches[0]).toEqual(
      expect.objectContaining({ nodeId: '1', numberOfBatches: expect.any(Number), duration: expect.any(Number) }),
    );
  });

  it('emits ACKNOWLEDGE with the acknowledged offsets and type once a fetch piggybacks them', async () => {
    // The ack for a record acquired on fetch #1 is only sent piggybacked on fetch #2's request
    // (there is no standalone ShareAcknowledge on the happy path) - so respond with the record
    // once, then empty responses, and only stop once that piggybacked ACKNOWLEDGE is observed.
    const shareFetch = vi
      .fn()
      .mockResolvedValueOnce({
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
      })
      .mockResolvedValue({ errorCode: 0, responses: [] });
    const shareAcknowledge = vi.fn().mockResolvedValue({ errorCode: 0, responses: [] });
    const findBroker = vi.fn().mockResolvedValue({ shareFetch, shareAcknowledge });
    const { shareGroup } = createShareGroup({
      cluster: {
        refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
        findTopicId: vi.fn().mockReturnValue(TOPIC_ID),
        findBroker,
      },
    });

    const instrumentationEmitter = new InstrumentationEventEmitter();
    const acks: ShareAcknowledgePayload[] = [];
    instrumentationEmitter.addListener(ACKNOWLEDGE, (event: InstrumentationEvent<unknown>) => {
      acks.push(event.payload as ShareAcknowledgePayload);
    });

    const runner = new ShareRunner({
      logger: silentLogger,
      shareGroup,
      heartbeatInterval: 50,
      maxWaitTimeInMs: 10,
      retry: { retries: 0 },
      instrumentationEmitter,
      eachMessage: async ({ message }) => {
        expect(message.offset).toBe(10n);
      },
      onCrash: vi.fn(),
    });

    await runner.start();
    await vi.waitFor(() => expect(acks.length).toBeGreaterThan(0));
    runner.shuttingDown = true;
    await runner.stop();

    expect(acks[0]).toEqual({
      groupId: 'share-1',
      memberId: 'member-1',
      nodeId: '1',
      topics: [
        {
          topic: 'events',
          partitions: [
            { partition: 0, firstOffset: 10n, lastOffset: 10n, acknowledgeType: SHARE_ACKNOWLEDGE_TYPE.ACCEPT },
          ],
        },
      ],
    });
  });

  it('emits ACKNOWLEDGE for the explicit ShareAcknowledge sent when a share session closes', async () => {
    // Mirrors the "fetches acquired records..." scenario: `eachMessage` shuts the runner down
    // synchronously as it handles the record, so the ACCEPT ack is queued but the runner never
    // gets another fetch round to piggyback it on - it is only flushed by `stop()`'s explicit
    // ShareAcknowledge (close-session) call.
    const { shareGroup, shareAcknowledge } = createShareGroup();
    const instrumentationEmitter = new InstrumentationEventEmitter();
    const acks: ShareAcknowledgePayload[] = [];
    instrumentationEmitter.addListener(ACKNOWLEDGE, (event: InstrumentationEvent<unknown>) => {
      acks.push(event.payload as ShareAcknowledgePayload);
    });

    const handled: bigint[] = [];
    const runner = new ShareRunner({
      logger: silentLogger,
      shareGroup,
      heartbeatInterval: 50,
      maxWaitTimeInMs: 10,
      retry: { retries: 0 },
      instrumentationEmitter,
      eachMessage: async ({ message }) => {
        handled.push(message.offset);
        runner.shuttingDown = true;
      },
      onCrash: vi.fn(),
    });

    await runner.start();
    await vi.waitFor(() => expect(handled).toEqual([10n]));
    await runner.stop();

    expect(shareAcknowledge).toHaveBeenCalled();
    expect(acks).toEqual([
      {
        groupId: 'share-1',
        memberId: 'member-1',
        nodeId: '1',
        topics: [
          {
            topic: 'events',
            partitions: [
              { partition: 0, firstOffset: 10n, lastOffset: 10n, acknowledgeType: SHARE_ACKNOWLEDGE_TYPE.ACCEPT },
            ],
          },
        ],
      },
    ]);
  });
});
