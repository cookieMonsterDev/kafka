import { describe, expect, it, vi, type Mock } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaRequestTimeoutError } from '../errors';
import { InstrumentationEventEmitter } from '../instrumentation/emitter';
import type { InstrumentationEvent } from '../instrumentation/event';
import { createLogger, LOG_LEVELS, type Logger } from '../loggers/index';
import { createErrorFromCode, ERROR_CODES } from '../protocol/error-codes';
import { SHARE_ACQUIRE_MODE } from '../protocol/requests/share-fetch/index';
import { SHARE_GROUP_JOIN_EPOCH } from '../protocol/requests/share-group-heartbeat/index';
import { SHARE_ACKNOWLEDGE_TYPE } from './acknowledge-types';
import { ACKNOWLEDGE, type ShareAcknowledgePayload } from './instrumentation-events';
import { ShareBatch } from './share-batch';
import { ShareGroup } from './share-group';
import { withSend } from '../broker/test-send';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });
const TOPIC_ID = Buffer.alloc(16, 9);
const { ACCEPT, RELEASE, RENEW } = SHARE_ACKNOWLEDGE_TYPE;

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

const RECORD = {
  magicByte: 2,
  attributes: 0,
  timestamp: 1n,
  offset: 10n,
  key: Buffer.from('k'),
  value: Buffer.from('v'),
  headers: {},
  isControlRecord: false,
  byteSize: 1,
  batchContext: BATCH_CONTEXT,
};
const ACQUIRED = { firstOffset: 10n, lastOffset: 10n, deliveryCount: 1 };
const EMPTY_RESPONSE = { errorCode: 0, responses: [] };

function partitionResponse(overrides: Record<string, unknown> = {}) {
  return {
    partitionIndex: 0,
    errorCode: 0,
    acknowledgeErrorCode: 0,
    currentLeader: { leaderId: -1, leaderEpoch: -1 },
    records: [RECORD],
    acquiredRecords: [ACQUIRED],
    ...overrides,
  };
}

function shareFetchResponse(...partitions: Record<string, unknown>[]) {
  return { errorCode: 0, responses: [{ topicId: TOPIC_ID, partitions }] };
}

interface ShareFetchRequest {
  groupId: string;
  memberId: string;
  shareSessionEpoch: number;
  isRenewAck: boolean;
  topics: {
    topicId: Buffer;
    partitions: { partitionIndex: number; acknowledgementBatches: { acknowledgeTypes: number[] }[] }[];
  }[];
  forgottenTopics: { topicId: Buffer; partitions: number[] }[];
}

function requestOf(shareFetch: Mock, call: number): ShareFetchRequest {
  const request = shareFetch.mock.calls[call]?.[0] as ShareFetchRequest | undefined;
  if (!request) throw new Error(`ShareFetch call ${call} was not made`);
  return request;
}

function acknowledgementsIn(request: ShareFetchRequest) {
  return request.topics.flatMap(({ partitions }) => partitions.flatMap((entry) => entry.acknowledgementBatches));
}

function createFetchingGroup({
  shareFetch = vi.fn().mockResolvedValue(EMPTY_RESPONSE),
  shareAcknowledge = vi.fn().mockResolvedValue(EMPTY_RESPONSE),
  findBroker,
  partitions = [0],
  retry = { retries: 0 },
  logger = silentLogger,
  shareAcquireMode,
}: {
  shareFetch?: Mock;
  shareAcknowledge?: Mock;
  findBroker?: Mock;
  partitions?: number[];
  retry?: { retries: number; initialRetryTime?: number; maxRetryTime?: number };
  logger?: Logger;
  shareAcquireMode?: 0 | 1;
} = {}) {
  const cluster = {
    refreshMetadata: vi.fn().mockResolvedValue(undefined),
    refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
    findTopicId: vi.fn().mockReturnValue(TOPIC_ID),
    findBroker: findBroker ?? vi.fn().mockResolvedValue(withSend({ shareFetch, shareAcknowledge })),
    findLeaderForPartitions: vi.fn((_topic: string, assigned: number[]) => ({ 1: assigned })),
  };
  const instrumentationEmitter = new InstrumentationEventEmitter();
  const acknowledged: ShareAcknowledgePayload[] = [];
  instrumentationEmitter.addListener(ACKNOWLEDGE, (event: InstrumentationEvent<unknown>) => {
    acknowledged.push(event.payload as ShareAcknowledgePayload);
  });
  const group = new ShareGroup({
    cluster: cluster as unknown as Cluster,
    groupId: 'share-1',
    logger,
    retry,
    maxWaitTimeInMs: 10,
    shareAcquireMode,
    instrumentationEmitter,
  });
  group.member.memberId = 'member-1';
  group.assignment = [{ topic: 'events', partitions }];
  return withSend({ group, cluster, shareFetch, shareAcknowledge, acknowledged });
}

function acquiredBatch(partition = 0): ShareBatch {
  return new ShareBatch('events', { partition, messages: [RECORD], acquiredRecords: [ACQUIRED] });
}

describe('share-consumer/share-group', () => {
  it('subscribes to topic names', () => {
    const group = new ShareGroup({
      cluster: {} as Cluster,
      groupId: 'share-1',
      logger: silentLogger,
    });
    group.subscribe(['events', 'clicks']);
    expect(group.topicsSubscribed).toEqual(['events', 'clicks']);
  });

  it('joins through ShareGroupHeartbeat until the broker assigns an epoch', async () => {
    const topicId = Buffer.alloc(16, 1);
    const shareGroupHeartbeat = vi
      .fn()
      .mockResolvedValueOnce({
        errorCode: 0,
        memberId: 'member-1',
        memberEpoch: SHARE_GROUP_JOIN_EPOCH,
        heartbeatIntervalMs: 50,
        assignment: null,
      })
      .mockResolvedValueOnce({
        errorCode: 0,
        memberId: 'member-1',
        memberEpoch: 1,
        heartbeatIntervalMs: 50,
        assignment: { topicPartitions: [{ topicId, partitions: [0, 1] }] },
      });
    const coordinator = withSend({ shareGroupHeartbeat });
    const cluster = withSend({
      connect: vi.fn(),
      disconnect: vi.fn(),
      findGroupCoordinator: vi.fn().mockResolvedValue(coordinator),
      refreshMetadata: vi.fn().mockResolvedValue(undefined),
    }) as unknown as Cluster;

    const group = new ShareGroup({ cluster, groupId: 'share-1', logger: silentLogger, retry: { retries: 0 } });
    group.subscribe(['events']);
    await group.joinAndSync();

    expect(group.member.isJoined).toBe(true);
    expect(group.memberId).toBe('member-1');
    expect(group.member.memberEpoch).toBe(1);
    expect(group.assigned()).toEqual([{ topic: 'events', partitions: [0, 1] }]);
    expect(shareGroupHeartbeat).toHaveBeenCalledTimes(2);
  });

  it('leaves with member epoch -1', async () => {
    const shareGroupHeartbeat = vi.fn().mockResolvedValue({
      errorCode: 0,
      memberId: 'member-1',
      memberEpoch: -1,
      heartbeatIntervalMs: 3000,
      assignment: null,
    });
    const coordinator = withSend({ shareGroupHeartbeat, nodeId: 1 });
    const group = new ShareGroup({
      cluster: withSend({ findGroupCoordinator: vi.fn().mockResolvedValue(coordinator) }) as unknown as Cluster,
      groupId: 'share-1',
      logger: silentLogger,
    });
    group.coordinator = coordinator as never;
    group.member.memberId = 'member-1';
    group.member.isJoined = true;

    await group.leave();
    expect(shareGroupHeartbeat).toHaveBeenCalledWith(
      expect.objectContaining({ groupId: 'share-1', memberId: 'member-1', memberEpoch: -1 }),
    );
    expect(group.member.isJoined).toBe(false);
  });

  it('connects and disconnects through the cluster, leaving when joined', async () => {
    const shareGroupHeartbeat = vi.fn().mockResolvedValue({
      errorCode: 0,
      memberId: 'member-1',
      memberEpoch: -1,
      heartbeatIntervalMs: 3000,
      assignment: null,
    });
    const connect = vi.fn();
    const disconnect = vi.fn();
    const cluster = withSend({
      connect,
      disconnect,
      findGroupCoordinator: vi.fn(),
    }) as unknown as Cluster;
    const group = new ShareGroup({ cluster, groupId: 'share-1', logger: silentLogger });
    group.coordinator = withSend({ shareGroupHeartbeat }) as never;
    group.member.memberId = 'member-1';
    group.member.isJoined = true;

    await group.connect();
    await group.disconnect();
    expect(connect).toHaveBeenCalled();
    expect(shareGroupHeartbeat).toHaveBeenCalled();
    expect(disconnect).toHaveBeenCalled();
    expect(group.member.isJoined).toBe(false);
  });

  it('leave is a no-op without a coordinator or member id', async () => {
    const group = new ShareGroup({ cluster: {} as Cluster, groupId: 'share-1', logger: silentLogger });
    await expect(group.leave()).resolves.toBeUndefined();
    group.coordinator = withSend({ shareGroupHeartbeat: vi.fn() }) as never;
    await expect(group.leave()).resolves.toBeUndefined();
  });

  it('clears membership when leave heartbeat fails', async () => {
    const shareGroupHeartbeat = vi.fn().mockRejectedValue(new Error('offline'));
    const group = new ShareGroup({ cluster: {} as Cluster, groupId: 'share-1', logger: silentLogger });
    group.coordinator = withSend({ shareGroupHeartbeat }) as never;
    group.member.memberId = 'member-1';
    group.member.isJoined = true;

    await group.leave();
    expect(group.member.isJoined).toBe(false);
    expect(group.member.memberEpoch).toBe(SHARE_GROUP_JOIN_EPOCH);
  });

  it('heartbeatDue is false without a coordinator and true after the interval elapses', () => {
    const group = new ShareGroup({ cluster: {} as Cluster, groupId: 'share-1', logger: silentLogger });
    expect(group.heartbeatDue(10)).toBe(false);
    group.coordinator = withSend({ shareGroupHeartbeat: vi.fn() }) as never;
    group.member.lastHeartbeatAt = 0;
    group.member.heartbeatIntervalMs = 1;
    expect(group.heartbeatDue(10_000)).toBe(true);
    group.member.lastHeartbeatAt = Date.now();
    group.member.heartbeatIntervalMs = 60_000;
    expect(group.heartbeatDue(60_000)).toBe(false);
  });

  it('skips a heartbeat that is not due unless forced', async () => {
    const shareGroupHeartbeat = vi.fn().mockResolvedValue({
      errorCode: 0,
      memberId: 'member-1',
      memberEpoch: 1,
      heartbeatIntervalMs: 60_000,
      assignment: null,
    });
    const group = new ShareGroup({ cluster: {} as Cluster, groupId: 'share-1', logger: silentLogger });
    group.coordinator = withSend({ shareGroupHeartbeat }) as never;
    group.member.memberId = 'member-1';
    group.member.memberEpoch = 1;
    group.member.lastHeartbeatAt = Date.now();
    group.member.heartbeatIntervalMs = 60_000;

    await group.heartbeat({ force: false });
    expect(shareGroupHeartbeat).not.toHaveBeenCalled();
    await group.heartbeat({ force: true });
    expect(shareGroupHeartbeat).toHaveBeenCalledTimes(1);
  });

  it('resolves assignment topic ids via ShareGroupDescribe when multiple topics are subscribed', async () => {
    const topicId = Buffer.alloc(16, 9);
    const shareGroupHeartbeat = vi.fn().mockResolvedValue({
      errorCode: 0,
      memberId: 'member-1',
      memberEpoch: 1,
      heartbeatIntervalMs: 50,
      assignment: { topicPartitions: [{ topicId, partitions: [2] }] },
    });
    const shareGroupDescribe = vi.fn().mockResolvedValue({
      groups: [
        {
          groupId: 'share-1',
          members: [{ assignment: { topicPartitions: [{ topicId, topicName: 'clicks', partitions: [2] }] } }],
        },
      ],
    });
    const coordinator = withSend({ shareGroupHeartbeat, shareGroupDescribe });
    const cluster = withSend({
      connect: vi.fn(),
      findGroupCoordinator: vi.fn().mockResolvedValue(coordinator),
      refreshMetadata: vi.fn().mockResolvedValue(undefined),
    }) as unknown as Cluster;
    const group = new ShareGroup({ cluster, groupId: 'share-1', logger: silentLogger, retry: { retries: 0 } });
    group.subscribe(['events', 'clicks']);
    await group.joinAndSync();

    expect(shareGroupDescribe).toHaveBeenCalled();
    expect(group.assigned()).toEqual([{ topic: 'clicks', partitions: [2] }]);
    expect(group.hasAssignment('clicks', 2)).toBe(true);
    expect(group.hasAssignment('events', 0)).toBe(false);
  });

  it('caches a resolved topic id so a later heartbeat does not re-describe', async () => {
    const topicId = Buffer.alloc(16, 3);
    const shareGroupHeartbeat = vi
      .fn()
      .mockResolvedValueOnce({
        errorCode: 0,
        memberId: 'member-1',
        memberEpoch: 1,
        heartbeatIntervalMs: 50,
        assignment: { topicPartitions: [{ topicId, partitions: [0] }] },
      })
      .mockResolvedValueOnce({
        errorCode: 0,
        memberId: 'member-1',
        memberEpoch: 1,
        heartbeatIntervalMs: 50,
        assignment: { topicPartitions: [{ topicId, partitions: [0, 1] }] },
      });
    const shareGroupDescribe = vi.fn().mockResolvedValue({
      groups: [
        {
          groupId: 'share-1',
          members: [{ assignment: { topicPartitions: [{ topicId, topicName: 'events', partitions: [0] }] } }],
        },
      ],
    });
    const coordinator = withSend({ shareGroupHeartbeat, shareGroupDescribe });
    const cluster = withSend({
      findGroupCoordinator: vi.fn().mockResolvedValue(coordinator),
      refreshMetadata: vi.fn().mockResolvedValue(undefined),
    }) as unknown as Cluster;
    const group = new ShareGroup({ cluster, groupId: 'share-1', logger: silentLogger, retry: { retries: 0 } });
    group.subscribe(['events', 'clicks']);
    await group.joinAndSync();
    await group.heartbeat({ force: true });
    expect(shareGroupDescribe).toHaveBeenCalledTimes(1);
    expect(group.assigned()).toEqual([{ topic: 'events', partitions: [0, 1] }]);
  });

  it('throws when a topic id cannot be resolved to a subscribed name', async () => {
    const topicId = Buffer.alloc(16, 4);
    const shareGroupHeartbeat = vi.fn().mockResolvedValue({
      errorCode: 0,
      memberId: 'member-1',
      memberEpoch: 1,
      heartbeatIntervalMs: 50,
      assignment: { topicPartitions: [{ topicId, partitions: [0] }] },
    });
    const shareGroupDescribe = vi.fn().mockResolvedValue({ groups: [] });
    const coordinator = withSend({ shareGroupHeartbeat, shareGroupDescribe });
    const cluster = withSend({
      findGroupCoordinator: vi.fn().mockResolvedValue(coordinator),
      refreshMetadata: vi.fn().mockResolvedValue(undefined),
    }) as unknown as Cluster;
    const group = new ShareGroup({ cluster, groupId: 'share-1', logger: silentLogger, retry: { retries: 0 } });
    group.subscribe(['events', 'clicks']);
    await expect(group.joinAndSync()).rejects.toThrow('Unable to resolve topic id');
  });

  it('filters assigned partitions by leader node', () => {
    const cluster = {
      findLeaderForPartitions: vi.fn((topic: string, partitions: number[]) => {
        if (topic === 'orders') return { 1: [0], 2: [1] };
        return { 1: partitions };
      }),
    } as unknown as Cluster;
    const group = new ShareGroup({ cluster, groupId: 'share-1', logger: silentLogger });
    group.assignment = [
      { topic: 'orders', partitions: [0, 1] },
      { topic: 'payments', partitions: [0] },
    ];

    expect(group.getNodeIds().sort()).toEqual(['1', '2']);
    expect(group.filterPartitionsByNode('1', group.assignment)).toEqual([
      { topic: 'orders', partitions: [0] },
      { topic: 'payments', partitions: [0] },
    ]);
    expect(group.filterPartitionsByNode('2', group.assignment)).toEqual([{ topic: 'orders', partitions: [1] }]);
    expect(group.filterPartitionsByNode('9', group.assignment)).toEqual([]);
  });

  it('re-finds the coordinator on NOT_COORDINATOR_FOR_GROUP during fetch recovery', async () => {
    const coordinator = withSend({ shareGroupHeartbeat: vi.fn() });
    const findGroupCoordinator = vi.fn().mockResolvedValue(coordinator);
    const cluster = withSend({ findGroupCoordinator }) as unknown as Cluster;
    const group = new ShareGroup({ cluster, groupId: 'share-1', logger: silentLogger });

    // Resolves rather than rethrowing, so the fetch loop can apply its own retry/crash policy.
    await expect(
      group.recoverFromFetch(Object.assign(new Error('moved'), { type: 'NOT_COORDINATOR_FOR_GROUP' })),
    ).resolves.toBeUndefined();
    expect(findGroupCoordinator).toHaveBeenCalledWith({ groupId: 'share-1' });
    expect(group.coordinator).toBe(coordinator);
  });

  it('ignores unrelated fetch errors during recovery', async () => {
    const findGroupCoordinator = vi.fn();
    const cluster = withSend({ findGroupCoordinator }) as unknown as Cluster;
    const group = new ShareGroup({ cluster, groupId: 'share-1', logger: silentLogger });
    await expect(
      group.recoverFromFetch(Object.assign(new Error('range'), { type: 'OFFSET_OUT_OF_RANGE' })),
    ).resolves.toBeUndefined();
    expect(findGroupCoordinator).not.toHaveBeenCalled();
  });

  describe('fetch', () => {
    it('opens a share session, then carries pending acknowledgements on the next ShareFetch', async () => {
      const shareFetch = vi
        .fn()
        .mockResolvedValueOnce(shareFetchResponse(partitionResponse()))
        .mockResolvedValue(EMPTY_RESPONSE);
      const { group, acknowledged } = createFetchingGroup(withSend({ shareFetch }));

      const [batch, ...rest] = await group.fetch('1');
      expect(rest).toEqual([]);
      expect(batch?.messages.map(({ offset }) => offset)).toEqual([10n]);
      group.acknowledge(batch!, ACCEPT);
      await group.fetch('1');

      expect(requestOf(shareFetch, 0)).toEqual(
        expect.objectContaining({
          groupId: 'share-1',
          memberId: 'member-1',
          shareSessionEpoch: 0,
          maxWaitMs: 10,
          shareAcquireMode: SHARE_ACQUIRE_MODE.BATCH_OPTIMIZED,
          isRenewAck: false,
          topics: [{ topicId: TOPIC_ID, partitions: [{ partitionIndex: 0, acknowledgementBatches: [] }] }],
          forgottenTopics: [],
        }),
      );
      expect(requestOf(shareFetch, 1).shareSessionEpoch).toBe(1);
      expect(requestOf(shareFetch, 1).topics).toEqual([
        {
          topicId: TOPIC_ID,
          partitions: [
            {
              partitionIndex: 0,
              acknowledgementBatches: [{ firstOffset: 10n, lastOffset: 10n, acknowledgeTypes: [ACCEPT] }],
            },
          ],
        },
      ]);
      expect(acknowledged).toEqual([
        {
          groupId: 'share-1',
          memberId: 'member-1',
          nodeId: '1',
          topics: [
            {
              topic: 'events',
              partitions: [{ partition: 0, firstOffset: 10n, lastOffset: 10n, acknowledgeType: ACCEPT }],
            },
          ],
        },
      ]);
    });

    it('marks a ShareFetch that carries a renew acknowledgement and sends the acquire mode', async () => {
      const { group, shareFetch } = createFetchingGroup({ shareAcquireMode: SHARE_ACQUIRE_MODE.RECORD_LIMIT });
      await group.fetch('1');
      group.acknowledge(acquiredBatch(), RENEW);
      await group.fetch('1');

      expect(requestOf(shareFetch, 0)).toEqual(
        expect.objectContaining({ isRenewAck: false, shareAcquireMode: SHARE_ACQUIRE_MODE.RECORD_LIMIT }),
      );
      expect(requestOf(shareFetch, 1).isRenewAck).toBe(true);
    });

    it('drops acknowledgements still pending when a new share session opens', async () => {
      const { group, shareFetch, acknowledged } = createFetchingGroup();
      group.acknowledge(acquiredBatch(), ACCEPT);
      await group.fetch('1');
      await group.fetch('1');

      expect(acknowledgementsIn(requestOf(shareFetch, 0))).toEqual([]);
      expect(acknowledgementsIn(requestOf(shareFetch, 1))).toEqual([]);
      expect(acknowledged).toEqual([]);
    });

    it('forgets a partition dropped from the assignment on the next ShareFetch to its node', async () => {
      const { group, shareFetch } = createFetchingGroup({ partitions: [0, 1] });
      await group.fetch('1');
      group.assignment = [{ topic: 'events', partitions: [0] }];
      await group.fetch('1');

      expect(requestOf(shareFetch, 0).forgottenTopics).toEqual([]);
      expect(requestOf(shareFetch, 1).forgottenTopics).toEqual([{ topicId: TOPIC_ID, partitions: [1] }]);
      expect(requestOf(shareFetch, 1).topics[0]?.partitions.map(({ partitionIndex }) => partitionIndex)).toEqual([0]);
    });

    it('opens a new share session after the broker rejects the current one, dropping its acknowledgements', async () => {
      const code = ERROR_CODES.find((entry) => entry.type === 'INVALID_SHARE_SESSION_EPOCH')!.code;
      const shareFetch = vi
        .fn()
        .mockResolvedValueOnce(EMPTY_RESPONSE)
        .mockRejectedValueOnce(createErrorFromCode(code))
        .mockResolvedValue(EMPTY_RESPONSE);
      const { group } = createFetchingGroup(withSend({ shareFetch }));

      await group.fetch('1');
      group.acknowledge(acquiredBatch(), ACCEPT);
      await expect(group.fetch('1')).rejects.toThrow();
      await group.fetch('1');

      expect(shareFetch.mock.calls.map(([request]) => (request as ShareFetchRequest).shareSessionEpoch)).toEqual([
        0, 1, 0,
      ]);
      expect(acknowledgementsIn(requestOf(shareFetch, 1))).toHaveLength(1);
      expect(acknowledgementsIn(requestOf(shareFetch, 2))).toEqual([]);
    });

    it('retries a failed ShareFetch on a new session instead of replaying its epoch and acknowledgements', async () => {
      const shareFetch = vi
        .fn()
        .mockResolvedValueOnce(EMPTY_RESPONSE)
        .mockRejectedValueOnce(new KafkaRequestTimeoutError('Request timed out', { broker: 'h:1' }))
        .mockResolvedValue(EMPTY_RESPONSE);
      const { group, acknowledged } = createFetchingGroup(
        withSend({
          shareFetch,
          retry: { retries: 1, initialRetryTime: 1, maxRetryTime: 5 },
        }),
      );

      await group.fetch('1');
      group.acknowledge(acquiredBatch(), RENEW);
      await group.fetch('1');
      await group.fetch('1');

      expect(requestOf(shareFetch, 1)).toEqual(expect.objectContaining({ shareSessionEpoch: 1, isRenewAck: true }));
      expect(acknowledgementsIn(requestOf(shareFetch, 1))).toHaveLength(1);
      // The broker may already have applied the timed-out request, so the retry neither repeats
      // its epoch nor its acknowledgements; it opens a new session instead.
      expect(requestOf(shareFetch, 2)).toEqual(expect.objectContaining({ shareSessionEpoch: 0, isRenewAck: false }));
      expect(acknowledgementsIn(requestOf(shareFetch, 2))).toEqual([]);
      expect(requestOf(shareFetch, 3).shareSessionEpoch).toBe(1);
      expect(acknowledgementsIn(requestOf(shareFetch, 3))).toEqual([]);
      expect(acknowledged).toEqual([]);
    });

    it('keeps acknowledgements for the next ShareFetch when a request never went out', async () => {
      const shareFetch = vi.fn().mockResolvedValue(EMPTY_RESPONSE);
      const broker = withSend({ shareFetch, shareAcknowledge: vi.fn() });
      const findBroker = vi
        .fn()
        .mockResolvedValueOnce(broker)
        .mockRejectedValueOnce(new Error('no broker'))
        .mockResolvedValue(broker);
      const { group } = createFetchingGroup(withSend({ shareFetch, findBroker }));

      await group.fetch('1');
      group.acknowledge(acquiredBatch(), ACCEPT);
      await expect(group.fetch('1')).rejects.toThrow('no broker');
      await group.fetch('1');

      expect(shareFetch).toHaveBeenCalledTimes(2);
      expect(requestOf(shareFetch, 1).shareSessionEpoch).toBe(1);
      expect(acknowledgementsIn(requestOf(shareFetch, 1))).toEqual([
        { firstOffset: 10n, lastOffset: 10n, acknowledgeTypes: [ACCEPT] },
      ]);
    });

    it('keeps healthy partitions, warns about failed acknowledgements, and refreshes metadata after stale leadership', async () => {
      const shareFetch = vi
        .fn()
        .mockResolvedValueOnce(
          shareFetchResponse(
            // NOT_LEADER_OR_FOLLOWER, INVALID_RECORD_STATE
            partitionResponse({ errorCode: 6, acknowledgeErrorCode: 121, records: [], acquiredRecords: [] }),
            partitionResponse({ partitionIndex: 1, acknowledgeErrorCode: 121 }),
          ),
        )
        .mockResolvedValue(EMPTY_RESPONSE);
      const warnings: Record<string, unknown>[] = [];
      const logger = createLogger({
        level: LOG_LEVELS.WARN,
        logCreator:
          () =>
          ({ log: { message: _message, ...extra } }) =>
            warnings.push(extra),
      });
      const { group, cluster } = createFetchingGroup(withSend({ shareFetch, partitions: [0, 1], logger }));

      const batches = await group.fetch('1');
      expect(batches.map(({ partition }) => partition)).toEqual([1]);
      expect(warnings).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ topic: 'events', partition: 1, error: expect.stringMatching(/record state/) }),
          expect.objectContaining({ topic: 'events', partition: 0, error: expect.stringMatching(/leader/i) }),
        ]),
      );

      expect(cluster.refreshMetadata).not.toHaveBeenCalled();
      await group.fetch('1');
      expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1);
    });

    it('rejects a response that names a topic id the request did not', async () => {
      const shareFetch = vi.fn().mockResolvedValue({
        errorCode: 0,
        responses: [{ topicId: Buffer.alloc(16, 7), partitions: [partitionResponse()] }],
      });
      const { group } = createFetchingGroup(withSend({ shareFetch }));

      await expect(group.fetch('1')).rejects.toThrow('Unknown topic id in share response');
    });

    it("throws a non-retriable partition error and releases the healthy partitions' records", async () => {
      const shareFetch = vi.fn().mockResolvedValue(
        shareFetchResponse(
          partitionResponse({ errorCode: 29, records: [], acquiredRecords: [] }), // TOPIC_AUTHORIZATION_FAILED
          partitionResponse({ partitionIndex: 1 }),
        ),
      );
      const { group, shareAcknowledge } = createFetchingGroup(withSend({ shareFetch, partitions: [0, 1] }));

      await expect(group.fetch('1')).rejects.toEqual(expect.objectContaining({ type: 'TOPIC_AUTHORIZATION_FAILED' }));
      await group.closeShareSessions();

      expect(shareAcknowledge).toHaveBeenCalledWith(
        expect.objectContaining({
          shareSessionEpoch: -1,
          topics: [
            {
              topicId: TOPIC_ID,
              partitions: [
                {
                  partitionIndex: 1,
                  acknowledgementBatches: [{ firstOffset: 10n, lastOffset: 10n, acknowledgeTypes: [RELEASE] }],
                },
              ],
            },
          ],
        }),
      );
    });
  });

  describe('closeShareSessions', () => {
    it('flushes pending acknowledgements with a closing ShareAcknowledge and emits ACKNOWLEDGE', async () => {
      const { group, shareFetch, shareAcknowledge, acknowledged } = createFetchingGroup();
      await group.fetch('1');
      group.acknowledge(acquiredBatch(), ACCEPT);
      await group.closeShareSessions();

      expect(shareAcknowledge).toHaveBeenCalledTimes(1);
      expect(shareAcknowledge).toHaveBeenCalledWith({
        groupId: 'share-1',
        memberId: 'member-1',
        shareSessionEpoch: -1,
        isRenewAck: false,
        topics: [
          {
            topicId: TOPIC_ID,
            partitions: [
              {
                partitionIndex: 0,
                acknowledgementBatches: [{ firstOffset: 10n, lastOffset: 10n, acknowledgeTypes: [ACCEPT] }],
              },
            ],
          },
        ],
      });
      expect(acknowledged).toEqual([
        {
          groupId: 'share-1',
          memberId: 'member-1',
          nodeId: '1',
          topics: [
            {
              topic: 'events',
              partitions: [{ partition: 0, firstOffset: 10n, lastOffset: 10n, acknowledgeType: ACCEPT }],
            },
          ],
        },
      ]);

      // The sessions are closed, so the next fetch opens a new one with nothing left to acknowledge.
      await group.fetch('1');
      expect(requestOf(shareFetch, 1).shareSessionEpoch).toBe(0);
      expect(acknowledgementsIn(requestOf(shareFetch, 1))).toEqual([]);
    });

    it('drops pending acknowledgements without a request when the member has no id', async () => {
      const { group, shareAcknowledge } = createFetchingGroup();
      group.acknowledge(acquiredBatch(), ACCEPT);
      group.member.memberId = null;
      await group.closeShareSessions();
      group.member.memberId = 'member-1';
      await group.closeShareSessions();

      expect(shareAcknowledge).toHaveBeenCalledTimes(1);
      expect(shareAcknowledge).toHaveBeenCalledWith(expect.objectContaining({ topics: [] }));
    });
  });
});
