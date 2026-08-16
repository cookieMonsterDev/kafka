import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Broker } from '../../src/broker/index.js';
import { MemberMetadata } from '../../src/consumer/assigner-protocol.js';
import { COORDINATOR_TYPES } from '../../src/protocol/enums/coordinator-types.js';
import {
  advertisedAddress,
  createConnectionPool,
  createTopic,
  newLogger,
  retryProtocol,
  secureRandom,
  TRANSIENT_METADATA_ERRORS,
} from '../helpers/index.js';

describe('broker.offsets', () => {
  let topicName: string;
  let groupId: string;
  let seedBroker: Broker | undefined;
  let leader: Broker | undefined;
  let coordinator: Broker | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    groupId = `consumer-group-id-${secureRandom()}`;
    seedBroker = new Broker({ connectionPool: createConnectionPool(), logger: newLogger() });
    await seedBroker.connect();
    await createTopic({ topic: topicName });
  });

  afterEach(async () => {
    await coordinator?.disconnect();
    await leader?.disconnect();
    await seedBroker?.disconnect();
  });

  it('lists, commits, and fetches offsets', async () => {
    const metadata = await retryProtocol(TRANSIENT_METADATA_ERRORS, () => seedBroker!.metadata([topicName]));
    const partition = metadata.topicMetadata[0]!.partitionMetadata[0]!;
    const brokerData = metadata.brokers.find((b) => b.nodeId === partition.leader)!;
    leader = new Broker({
      connectionPool: createConnectionPool(advertisedAddress(brokerData.host, brokerData.port)),
      logger: newLogger(),
    });
    await leader.connect();

    await leader.produce({
      acks: 1,
      timeout: 30_000,
      topicData: [{ topic: topicName, partitions: [{ partition: 0, messages: [{ key: 'k', value: 'v' }] }] }],
    });

    const listed = await leader.listOffsets({
      topics: [{ topic: topicName, partitions: [{ partition: 0, timestamp: -1n }] }],
    });
    expect(listed.responses[0]?.partitions[0]?.offset).toBeGreaterThanOrEqual(0n);

    const dest = await retryProtocol('GROUP_COORDINATOR_NOT_AVAILABLE', () =>
      seedBroker!.findGroupCoordinator({
        coordinatorKey: groupId,
        coordinatorType: COORDINATOR_TYPES.GROUP,
      }),
    );
    coordinator = new Broker({
      connectionPool: createConnectionPool(advertisedAddress(dest.coordinator.host, dest.coordinator.port)),
      logger: newLogger(),
    });
    await coordinator.connect();

    const join = await retryProtocol(['NOT_COORDINATOR_FOR_GROUP', 'GROUP_LOAD_IN_PROGRESS'], () =>
      coordinator.joinGroup({
        groupId,
        sessionTimeout: 30_000,
        rebalanceTimeout: 60_000,
        memberId: '',
        protocolType: 'consumer',
        groupProtocols: [{ name: 'range', metadata: MemberMetadata.encode({ version: 1, topics: [topicName] }) }],
      }),
    );

    await coordinator.syncGroup({
      groupId,
      generationId: join.generationId,
      memberId: join.memberId,
      groupAssignment: [{ memberId: join.memberId, memberAssignment: Buffer.alloc(0) }],
    });

    const committed = await coordinator.offsetCommit({
      groupId,
      groupGenerationId: join.generationId,
      memberId: join.memberId,
      topics: [{ topic: topicName, partitions: [{ partition: 0, offset: 1n }] }],
    });
    expect(committed.responses[0]?.partitions[0]?.errorCode).toBe(0);

    const fetched = await coordinator.offsetFetch({
      groupId,
      topics: [{ topic: topicName, partitions: [{ partition: 0 }] }],
    });
    expect(fetched.responses[0]?.partitions[0]?.offset).toBe(1n);

    await coordinator.leaveGroup({ groupId, memberId: join.memberId });
  });
});
