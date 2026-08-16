import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaAggregateError, KafkaNonRetriableError, KafkaProtocolError } from '../errors';
import { InstrumentationEventEmitter } from '../instrumentation/emitter';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { NETWORK_REQUEST } from '../network/instrumentation-events';
import { ACL_OPERATION_TYPES } from '../protocol/enums/acl-operation-types';
import { ACL_PERMISSION_TYPES } from '../protocol/enums/acl-permission-types';
import { ACL_RESOURCE_TYPES } from '../protocol/enums/acl-resource-types';
import { RESOURCE_PATTERN_TYPES } from '../protocol/enums/resource-pattern-types';
import { CONFIG_RESOURCE_TYPES } from '../protocol/enums/config-resource-types';
import { INCREMENTAL_ALTER_CONFIGS_OPERATIONS } from '../protocol/enums/incremental-alter-configs-operations';
import { createAdmin } from './index';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function fakeBroker(overrides: Record<string, unknown> = {}) {
  return {
    nodeId: 1,
    createTopics: vi.fn().mockResolvedValue({}),
    deleteTopics: vi.fn().mockResolvedValue({}),
    createPartitions: vi.fn().mockResolvedValue({}),
    metadata: vi.fn().mockResolvedValue({ topicMetadata: [] }),
    describeConfigs: vi.fn().mockResolvedValue({ resources: [] }),
    alterConfigs: vi.fn().mockResolvedValue({ resources: [] }),
    incrementalAlterConfigs: vi.fn().mockResolvedValue({ resources: [] }),
    listGroups: vi.fn().mockResolvedValue({ groups: [] }),
    describeGroups: vi.fn().mockResolvedValue({ groups: [] }),
    deleteGroups: vi.fn().mockResolvedValue({ results: [] }),
    offsetDelete: vi.fn().mockResolvedValue({ topics: [] }),
    offsetFetch: vi.fn().mockResolvedValue({ responses: [] }),
    createAcls: vi.fn().mockResolvedValue({}),
    describeAcls: vi.fn().mockResolvedValue({ resources: [] }),
    deleteAcls: vi.fn().mockResolvedValue({ filterResponses: [] }),
    deleteRecords: vi.fn().mockResolvedValue({}),
    alterPartitionReassignments: vi.fn().mockResolvedValue({}),
    listPartitionReassignments: vi.fn().mockResolvedValue({ topics: [] }),
    electLeaders: vi.fn().mockResolvedValue({ results: [] }),
    describeUserScramCredentials: vi.fn().mockResolvedValue({ results: [] }),
    alterUserScramCredentials: vi.fn().mockResolvedValue({ results: [] }),
    describeClientQuotas: vi.fn().mockResolvedValue({ entries: [] }),
    alterClientQuotas: vi.fn().mockResolvedValue({ entries: [] }),
    ...overrides,
  };
}

function fakeCluster(overrides: Record<string, unknown> = {}) {
  const broker = fakeBroker();
  return {
    connect: vi.fn().mockResolvedValue(undefined),
    disconnect: vi.fn().mockResolvedValue(undefined),
    refreshMetadata: vi.fn().mockResolvedValue(undefined),
    refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
    findControllerBroker: vi.fn().mockResolvedValue(broker),
    findBroker: vi.fn().mockResolvedValue(broker),
    findGroupCoordinator: vi.fn().mockResolvedValue(broker),
    metadata: vi.fn().mockResolvedValue({
      topicMetadata: [],
      brokers: [],
      clusterId: 'cluster',
      controllerId: 1,
    }),
    addTargetTopic: vi.fn().mockResolvedValue(undefined),
    findTopicPartitionMetadata: vi.fn().mockReturnValue([]),
    fetchTopicsOffset: vi.fn().mockResolvedValue([]),
    defaultOffset: vi.fn(({ fromBeginning }: { fromBeginning?: boolean }) => (fromBeginning ? -2n : -1n)),
    findLeaderForPartitions: vi.fn().mockReturnValue({}),
    targetTopics: new Set<string>(),
    brokerPool: { brokers: {} },
    _broker: broker,
    ...overrides,
  };
}

describe('admin', () => {
  it('exposes a namespaced logger', () => {
    const admin = createAdmin({ cluster: fakeCluster() as unknown as Cluster, logger: silentLogger });
    expect(typeof admin.logger().info).toBe('function');
  });

  it('emits connect/disconnect events', async () => {
    const cluster = fakeCluster();
    const admin = createAdmin({ cluster: cluster as unknown as Cluster, logger: silentLogger });
    const connectListener = vi.fn();
    const disconnectListener = vi.fn();
    admin.on(admin.events.CONNECT, connectListener);
    admin.on(admin.events.DISCONNECT, disconnectListener);

    await admin.connect();
    expect(connectListener).toHaveBeenCalled();
    expect(cluster.connect).toHaveBeenCalled();

    await admin.disconnect();
    expect(disconnectListener).toHaveBeenCalled();
    expect(cluster.disconnect).toHaveBeenCalled();
  });

  it('rejects connect when the signal is already aborted', async () => {
    const cluster = fakeCluster();
    const admin = createAdmin({ cluster: cluster as unknown as Cluster, logger: silentLogger });
    await expect(admin.connect({ signal: AbortSignal.abort() })).rejects.toThrow(/aborted/i);
    expect(cluster.connect).not.toHaveBeenCalled();
  });

  it('disconnects through Symbol.asyncDispose', async () => {
    const cluster = fakeCluster();
    const admin = createAdmin({ cluster: cluster as unknown as Cluster, logger: silentLogger });
    await admin[Symbol.asyncDispose]();
    expect(cluster.disconnect).toHaveBeenCalled();
  });

  it('rejects an unknown event name', () => {
    const admin = createAdmin({ cluster: fakeCluster() as unknown as Cluster, logger: silentLogger });
    expect(() => admin.on('NON_EXISTENT_EVENT' as never, () => {})).toThrow(
      /Event name should be one of admin\.events\./,
    );
  });

  it('forwards network request events, rewriting the event type back to the public name', () => {
    const emitter = new InstrumentationEventEmitter();
    const admin = createAdmin({
      cluster: fakeCluster() as unknown as Cluster,
      logger: silentLogger,
      instrumentationEmitter: emitter,
    });

    const requestListener = vi.fn();
    admin.on(admin.events.REQUEST, requestListener);
    emitter.emit(NETWORK_REQUEST, { apiName: 'Metadata' });

    expect(requestListener).toHaveBeenCalledWith(
      expect.objectContaining({ type: 'admin.network.request', payload: { apiName: 'Metadata' } }),
    );
  });

  it('lists topics from cluster metadata', async () => {
    const cluster = fakeCluster({
      metadata: vi.fn().mockResolvedValue({
        topicMetadata: [{ topic: 'a' }, { topic: 'b' }],
      }),
    });
    const admin = createAdmin({ cluster: cluster as unknown as Cluster, logger: silentLogger });
    await expect(admin.listTopics()).resolves.toEqual(['a', 'b']);
  });

  it('rejects an invalid createTopics payload', async () => {
    const admin = createAdmin({ cluster: fakeCluster() as unknown as Cluster, logger: silentLogger });
    await expect(admin.createTopics({ topics: null as never })).rejects.toThrow('Invalid topics array null');
    await expect(admin.createTopics({ topics: [{ topic: 123 as never }] })).rejects.toThrow(
      'Invalid topics array, the topic names have to be a valid string',
    );
    await expect(admin.createTopics({ topics: [{ topic: 't' }, { topic: 't' }] })).rejects.toThrow(
      'Invalid topics array, it cannot have multiple entries for the same topic',
    );
  });

  it('rejects invalid configEntries on createTopics', async () => {
    const admin = createAdmin({ cluster: fakeCluster() as unknown as Cluster, logger: silentLogger });
    await expect(admin.createTopics({ topics: [{ topic: 't', configEntries: 'nope' as never }] })).rejects.toThrow(
      'Invalid configEntries for topic "t", must be an array',
    );
    await expect(
      admin.createTopics({ topics: [{ topic: 't', configEntries: [{ name: 'retention.ms' } as never] }] }),
    ).rejects.toThrow('must have a valid "value" property');
  });

  it('creates topics through the controller broker', async () => {
    const cluster = fakeCluster();
    const admin = createAdmin({ cluster: cluster as unknown as Cluster, logger: silentLogger });
    await expect(admin.createTopics({ topics: [{ topic: 'orders' }], waitForLeaders: false })).resolves.toBe(true);
    expect(cluster.findControllerBroker).toHaveBeenCalled();
    expect(cluster._broker.createTopics).toHaveBeenCalledWith({
      topics: [{ topic: 'orders' }],
      validateOnly: undefined,
      timeout: undefined,
    });
  });

  it('returns false when every createTopics error is TOPIC_ALREADY_EXISTS', async () => {
    const alreadyExists = new KafkaProtocolError({
      message: 'Topic already exists',
      type: 'TOPIC_ALREADY_EXISTS',
      code: 36,
      retriable: false,
    });
    const cluster = fakeCluster({
      findControllerBroker: vi.fn().mockResolvedValue(
        fakeBroker({
          createTopics: vi.fn().mockRejectedValue(new KafkaAggregateError('Failed', [alreadyExists])),
        }),
      ),
    });
    const admin = createAdmin({ cluster: cluster as unknown as Cluster, logger: silentLogger });
    await expect(admin.createTopics({ topics: [{ topic: 'orders' }], waitForLeaders: false })).resolves.toBe(false);
  });

  it('describes the cluster, treating controller id -1 as unknown', async () => {
    const cluster = fakeCluster({
      metadata: vi.fn().mockResolvedValue({
        brokers: [{ nodeId: 1, host: 'localhost', port: 9092, rack: null }],
        clusterId: 'cid',
        controllerId: -1,
        topicMetadata: [],
      }),
    });
    const admin = createAdmin({ cluster: cluster as unknown as Cluster, logger: silentLogger });
    await expect(admin.describeCluster()).resolves.toEqual({
      brokers: [{ nodeId: 1, host: 'localhost', port: 9092 }],
      controller: null,
      clusterId: 'cid',
    });
  });

  it('fetches topic offsets as bigint high/low watermarks', async () => {
    const cluster = fakeCluster({
      findTopicPartitionMetadata: vi.fn().mockReturnValue([{ partitionId: 0 }]),
      fetchTopicsOffset: vi
        .fn()
        .mockResolvedValueOnce([{ topic: 't', partitions: [{ partition: 0, offset: 10n }] }])
        .mockResolvedValueOnce([{ topic: 't', partitions: [{ partition: 0, offset: 0n }] }]),
    });
    const admin = createAdmin({ cluster: cluster as unknown as Cluster, logger: silentLogger });
    await expect(admin.fetchTopicOffsets('t')).resolves.toEqual([{ partition: 0, offset: 10n, high: 10n, low: 0n }]);
  });

  it('rejects fetchTopicOffsets without a topic name', async () => {
    const admin = createAdmin({ cluster: fakeCluster() as unknown as Cluster, logger: silentLogger });
    await expect(admin.fetchTopicOffsets(null as never)).rejects.toThrow('Invalid topic null');
  });

  it('falls back to the high watermark when a timestamp lookup returns a negative offset', async () => {
    const cluster = fakeCluster({
      findTopicPartitionMetadata: vi.fn().mockReturnValue([{ partitionId: 0 }]),
      fetchTopicsOffset: vi
        .fn()
        .mockResolvedValueOnce([{ topic: 't', partitions: [{ partition: 0, offset: 42n }] }])
        .mockResolvedValueOnce([{ topic: 't', partitions: [{ partition: 0, offset: -1n }] }]),
    });
    const admin = createAdmin({ cluster: cluster as unknown as Cluster, logger: silentLogger });
    await expect(admin.fetchTopicOffsetsByTimestamp('t', 1n)).resolves.toEqual([{ partition: 0, offset: 42n }]);
  });

  it('describes configs through the controller for topic resources', async () => {
    const broker = fakeBroker({
      describeConfigs: vi.fn().mockResolvedValue({ resources: [{ resourceName: 'orders' }] }),
    });
    const cluster = fakeCluster({ findControllerBroker: vi.fn().mockResolvedValue(broker) });
    const admin = createAdmin({ cluster: cluster as unknown as Cluster, logger: silentLogger });
    await expect(
      admin.describeConfigs({
        resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: 'orders' }],
        includeDocumentation: true,
      }),
    ).resolves.toEqual({ resources: [{ resourceName: 'orders' }] });
    expect(broker.describeConfigs).toHaveBeenCalledWith(expect.objectContaining({ includeDocumentation: true }));
  });

  it('routes broker config queries to the named broker', async () => {
    const controller = fakeBroker({ nodeId: 1 });
    const target = fakeBroker({
      nodeId: 2,
      describeConfigs: vi.fn().mockResolvedValue({ resources: [{ resourceName: '2' }] }),
    });
    const cluster = fakeCluster({
      findControllerBroker: vi.fn().mockResolvedValue(controller),
      findBroker: vi.fn().mockResolvedValue(target),
    });
    const admin = createAdmin({ cluster: cluster as unknown as Cluster, logger: silentLogger });
    await admin.describeConfigs({
      resources: [{ type: CONFIG_RESOURCE_TYPES.BROKER, name: '2', configNames: ['log.retention.hours'] }],
    });
    expect(cluster.findBroker).toHaveBeenCalledWith({ nodeId: '2' });
    expect(target.describeConfigs).toHaveBeenCalled();
    expect(controller.describeConfigs).not.toHaveBeenCalled();
  });

  it('rejects an empty describeConfigs resources array', async () => {
    const admin = createAdmin({ cluster: fakeCluster() as unknown as Cluster, logger: silentLogger });
    await expect(admin.describeConfigs({ resources: [] })).rejects.toThrow('Resources array cannot be empty');
  });

  it('incrementally alters configs through the controller', async () => {
    const broker = fakeBroker({
      incrementalAlterConfigs: vi.fn().mockResolvedValue({ resources: [{ resourceName: 'orders' }] }),
    });
    const cluster = fakeCluster({ findControllerBroker: vi.fn().mockResolvedValue(broker) });
    const admin = createAdmin({ cluster: cluster as unknown as Cluster, logger: silentLogger });
    await expect(
      admin.incrementalAlterConfigs({
        resources: [
          {
            type: CONFIG_RESOURCE_TYPES.TOPIC,
            name: 'orders',
            configs: [
              { name: 'cleanup.policy', value: 'compact', operation: INCREMENTAL_ALTER_CONFIGS_OPERATIONS.SET },
            ],
          },
        ],
      }),
    ).resolves.toEqual({ resources: [{ resourceName: 'orders' }] });
    expect(broker.incrementalAlterConfigs).toHaveBeenCalled();
  });

  it('rejects an empty incrementalAlterConfigs resources array', async () => {
    const admin = createAdmin({ cluster: fakeCluster() as unknown as Cluster, logger: silentLogger });
    await expect(admin.incrementalAlterConfigs({ resources: [] })).rejects.toThrow('Resources array cannot be empty');
  });

  it('lists groups from every broker in the pool', async () => {
    const brokerA = fakeBroker({
      nodeId: 1,
      listGroups: vi.fn().mockResolvedValue({ groups: [{ groupId: 'g1', protocolType: 'consumer' }] }),
    });
    const brokerB = fakeBroker({
      nodeId: 2,
      listGroups: vi.fn().mockResolvedValue({ groups: [{ groupId: 'g2', protocolType: 'consumer' }] }),
    });
    const cluster = fakeCluster({
      brokerPool: { brokers: { '1': brokerA, '2': brokerB } },
      findBroker: vi.fn(async ({ nodeId }: { nodeId: string }) => (nodeId === '1' ? brokerA : brokerB)),
    });
    const admin = createAdmin({ cluster: cluster as unknown as Cluster, logger: silentLogger });
    await expect(admin.listGroups()).resolves.toEqual({
      groups: [
        { groupId: 'g1', protocolType: 'consumer' },
        { groupId: 'g2', protocolType: 'consumer' },
      ],
    });
  });

  it('rejects an invalid deleteGroups payload', async () => {
    const admin = createAdmin({ cluster: fakeCluster() as unknown as Cluster, logger: silentLogger });
    await expect(admin.deleteGroups(null as never)).rejects.toThrow('Invalid groupIds array null');
    await expect(admin.deleteGroups([1 as never])).rejects.toThrow('Invalid groupId name: true');
  });

  it('deletes group offsets through the group coordinator', async () => {
    const broker = fakeBroker({
      offsetDelete: vi.fn().mockResolvedValue({ topics: [{ name: 'orders', partitions: [] }] }),
    });
    const cluster = fakeCluster({ findGroupCoordinator: vi.fn().mockResolvedValue(broker) });
    const admin = createAdmin({ cluster: cluster as unknown as Cluster, logger: silentLogger });
    await expect(
      admin.deleteGroupOffsets({ groupId: 'g', topics: [{ topic: 'orders', partitions: [0] }] }),
    ).resolves.toEqual({ topics: [{ name: 'orders', partitions: [] }] });
    expect(cluster.findGroupCoordinator).toHaveBeenCalledWith({ groupId: 'g' });
    expect(broker.offsetDelete).toHaveBeenCalledWith({ groupId: 'g', topics: [{ topic: 'orders', partitions: [0] }] });
  });

  it('rejects a missing deleteGroupOffsets groupId', async () => {
    const admin = createAdmin({ cluster: fakeCluster() as unknown as Cluster, logger: silentLogger });
    await expect(admin.deleteGroupOffsets({ groupId: '', topics: [] })).rejects.toThrow('Invalid groupId');
  });

  it('creates ACLs through the controller, remapping acl to creations', async () => {
    const broker = fakeBroker();
    const cluster = fakeCluster({ findControllerBroker: vi.fn().mockResolvedValue(broker) });
    const admin = createAdmin({ cluster: cluster as unknown as Cluster, logger: silentLogger });
    const acl = [
      {
        principal: 'User:alice',
        host: '*',
        operation: ACL_OPERATION_TYPES.READ,
        permissionType: ACL_PERMISSION_TYPES.ALLOW,
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: 'orders',
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
      },
    ];
    await expect(admin.createAcls({ acl })).resolves.toBe(true);
    expect(broker.createAcls).toHaveBeenCalledWith({ creations: acl });
  });

  it('rejects an empty ACL array', async () => {
    const admin = createAdmin({ cluster: fakeCluster() as unknown as Cluster, logger: silentLogger });
    await expect(admin.createAcls({ acl: [] })).rejects.toThrow('Empty ACL array');
  });

  it('rejects alterPartitionReassignments with a negative replica', async () => {
    const admin = createAdmin({ cluster: fakeCluster() as unknown as Cluster, logger: silentLogger });
    await expect(
      admin.alterPartitionReassignments({
        topics: [{ topic: 't', partitionAssignment: [{ partition: 0, replicas: [-1] }] }],
      }),
    ).rejects.toThrow(/Replicas must be a non negative number/);
  });

  it('lists partition reassignments through the controller', async () => {
    const broker = fakeBroker({
      listPartitionReassignments: vi.fn().mockResolvedValue({ topics: [{ name: 't', partitions: [] }] }),
    });
    const cluster = fakeCluster({ findControllerBroker: vi.fn().mockResolvedValue(broker) });
    const admin = createAdmin({ cluster: cluster as unknown as Cluster, logger: silentLogger });
    await expect(admin.listPartitionReassignments()).resolves.toEqual({ topics: [{ name: 't', partitions: [] }] });
  });

  it('elects leaders through the controller', async () => {
    const broker = fakeBroker({
      electLeaders: vi.fn().mockResolvedValue({ results: [{ topic: 'orders', partitions: [] }] }),
    });
    const cluster = fakeCluster({ findControllerBroker: vi.fn().mockResolvedValue(broker) });
    const admin = createAdmin({ cluster: cluster as unknown as Cluster, logger: silentLogger });
    await expect(admin.electLeaders({ topicPartitions: [{ topic: 'orders', partitions: [0] }] })).resolves.toEqual({
      results: [{ topic: 'orders', partitions: [] }],
    });
    expect(broker.electLeaders).toHaveBeenCalledWith({
      topicPartitions: [{ topic: 'orders', partitions: [0] }],
      electionType: undefined,
      timeout: undefined,
    });
  });

  it('describes SCRAM credentials through the controller', async () => {
    const broker = fakeBroker({
      describeUserScramCredentials: vi.fn().mockResolvedValue({ results: [{ user: 'alice', credentialInfos: [] }] }),
    });
    const cluster = fakeCluster({ findControllerBroker: vi.fn().mockResolvedValue(broker) });
    const admin = createAdmin({ cluster: cluster as unknown as Cluster, logger: silentLogger });
    await expect(admin.describeUserScramCredentials({ users: ['alice'] })).resolves.toEqual({
      results: [{ user: 'alice', credentialInfos: [] }],
    });
    expect(broker.describeUserScramCredentials).toHaveBeenCalledWith({ users: ['alice'] });
  });

  it('alters SCRAM credentials with a precomputed salted password', async () => {
    const broker = fakeBroker({
      alterUserScramCredentials: vi.fn().mockResolvedValue({ results: [{ user: 'alice', errorCode: 0 }] }),
    });
    const cluster = fakeCluster({ findControllerBroker: vi.fn().mockResolvedValue(broker) });
    const admin = createAdmin({ cluster: cluster as unknown as Cluster, logger: silentLogger });
    const salt = Buffer.from([1, 2, 3, 4]);
    const saltedPassword = Buffer.from([5, 6, 7, 8]);
    await expect(
      admin.alterUserScramCredentials({
        upsertions: [{ name: 'alice', mechanism: 1, iterations: 4096, salt, saltedPassword }],
      }),
    ).resolves.toEqual({ results: [{ user: 'alice', errorCode: 0 }] });
    expect(broker.alterUserScramCredentials).toHaveBeenCalledWith({
      deletions: [],
      upsertions: [{ name: 'alice', mechanism: 1, iterations: 4096, salt, saltedPassword }],
    });
  });

  it('rejects empty SCRAM alterations', async () => {
    const admin = createAdmin({ cluster: fakeCluster() as unknown as Cluster, logger: silentLogger });
    await expect(admin.alterUserScramCredentials({})).rejects.toThrow(KafkaNonRetriableError);
  });

  it('describes client quotas through the controller', async () => {
    const broker = fakeBroker({
      describeClientQuotas: vi.fn().mockResolvedValue({ entries: [{ entity: [], values: [] }] }),
    });
    const cluster = fakeCluster({ findControllerBroker: vi.fn().mockResolvedValue(broker) });
    const admin = createAdmin({ cluster: cluster as unknown as Cluster, logger: silentLogger });
    await expect(admin.describeClientQuotas({ components: [], strict: true })).resolves.toEqual({
      entries: [{ entity: [], values: [] }],
    });
    expect(broker.describeClientQuotas).toHaveBeenCalledWith({ components: [], strict: true });
  });

  it('rejects an empty alterClientQuotas entries array', async () => {
    const admin = createAdmin({ cluster: fakeCluster() as unknown as Cluster, logger: silentLogger });
    await expect(admin.alterClientQuotas({ entries: [] })).rejects.toThrow('Entries array cannot be empty');
  });

  it('throws KafkaNonRetriableError for a missing setOffsets groupId', async () => {
    const admin = createAdmin({ cluster: fakeCluster() as unknown as Cluster, logger: silentLogger });
    await expect(
      admin.setOffsets({ groupId: '', topic: 't', partitions: [{ partition: 0, offset: 1n }] }),
    ).rejects.toThrow(KafkaNonRetriableError);
  });
});
