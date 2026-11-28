import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Broker } from '../../../src/broker/index';
import { MemberMetadata } from '../../../src/consumer/assigner-protocol';
import { COORDINATOR_TYPES } from '../../../src/protocol/enums/coordinator-types';
import { EARLIEST_OFFSET, LATEST_OFFSET } from '../../../src/constants';
import {
  advertisedAddress,
  createConnectionPool,
  createTopic,
  newLogger,
  retryProtocol,
  secureRandom,
  testIfKafkaAtLeast_4_2,
  TRANSIENT_METADATA_ERRORS,
} from '../../helpers/index';
import { API_KEYS } from '../../../src/protocol/requests/api-keys';
import { FindCoordinator } from '../../../src/protocol/requests/find-coordinator/index';
import { JoinGroup } from '../../../src/protocol/requests/join-group/index';
import { LeaveGroup } from '../../../src/protocol/requests/leave-group/index';
import { ListOffsets } from '../../../src/protocol/requests/list-offsets/index';
import { Metadata } from '../../../src/protocol/requests/metadata/index';
import { OffsetCommit } from '../../../src/protocol/requests/offset-commit/index';
import { OffsetFetch } from '../../../src/protocol/requests/offset-fetch/index';
import { Produce } from '../../../src/protocol/requests/produce/index';
import { SyncGroup } from '../../../src/protocol/requests/sync-group/index';

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
    const metadata = await retryProtocol(TRANSIENT_METADATA_ERRORS, () =>
      seedBroker!.send(Metadata, { topics: [topicName], allowAutoTopicCreation: true }),
    );
    const partition = metadata.topicMetadata[0]!.partitionMetadata[0]!;
    const brokerData = metadata.brokers.find((b) => b.nodeId === partition.leader)!;
    leader = new Broker({
      connectionPool: createConnectionPool(advertisedAddress(brokerData.host, brokerData.port)),
      logger: newLogger(),
    });
    await leader.connect();

    await retryProtocol(TRANSIENT_METADATA_ERRORS, () =>
      leader!.send(Produce, {
        acks: 1,
        timeout: 30_000,
        topicData: [{ topic: topicName, partitions: [{ partition: 0, messages: [{ key: 'k', value: 'v' }] }] }],
      }),
    );

    const listed = await leader.send(ListOffsets, {
      topics: [{ topic: topicName, partitions: [{ partition: 0, timestamp: -1n }] }],
    });
    expect(listed.responses[0]?.partitions[0]?.offset).toBeGreaterThanOrEqual(0n);

    const {
      coordinators: [dest],
    } = await retryProtocol('GROUP_COORDINATOR_NOT_AVAILABLE', () =>
      seedBroker!.send(FindCoordinator, {
        coordinatorKeys: [groupId],
        coordinatorType: COORDINATOR_TYPES.GROUP,
      }),
    );
    coordinator = new Broker({
      connectionPool: createConnectionPool(advertisedAddress(dest!.host, dest!.port)),
      logger: newLogger(),
    });
    await coordinator.connect();

    const join = await retryProtocol(['NOT_COORDINATOR_FOR_GROUP', 'GROUP_LOAD_IN_PROGRESS'], () =>
      coordinator!.send(JoinGroup, {
        groupId,
        sessionTimeout: 30_000,
        rebalanceTimeout: 60_000,
        memberId: '',
        protocolType: 'consumer',
        groupProtocols: [{ name: 'range', metadata: MemberMetadata.encode({ version: 1, topics: [topicName] }) }],
      }),
    );

    await coordinator.send(SyncGroup, {
      groupId,
      generationId: join.generationId,
      memberId: join.memberId,
      protocolType: 'consumer',
      protocolName: join.groupProtocol,
      groupAssignment: [{ memberId: join.memberId, memberAssignment: Buffer.alloc(0) }],
    });

    const committed = await coordinator.send(OffsetCommit, {
      groupId,
      groupGenerationId: join.generationId,
      memberId: join.memberId,
      topics: [{ topic: topicName, partitions: [{ partition: 0, offset: 1n }] }],
    });
    expect(committed.responses[0]?.partitions[0]?.errorCode).toBe(0);

    const fetched = await coordinator.send(OffsetFetch, {
      groups: [{ groupId, topics: [{ topic: topicName, partitions: [{ partition: 0 }] }] }],
    });
    expect(fetched.groups[0]?.topics[0]?.partitions[0]?.offset).toBe(1n);

    // With a topic id, Kafka 4.2+ takes OffsetCommit and OffsetFetch v10; older brokers stay on names.
    const topicId = metadata.topicMetadata[0]?.topicId;
    const committedById = await coordinator.send(OffsetCommit, {
      groupId,
      groupGenerationId: join.generationId,
      memberId: join.memberId,
      topics: [{ topic: topicName, topicId, partitions: [{ partition: 0, offset: 2n }] }],
    });
    expect(committedById.responses[0]).toMatchObject({
      topic: topicName,
      partitions: [{ partition: 0, errorCode: 0 }],
    });

    const fetchedById = await coordinator.send(OffsetFetch, {
      groups: [{ groupId, topics: [{ topic: topicName, topicId, partitions: [{ partition: 0 }] }] }],
    });
    expect(fetchedById.groups[0]?.topics[0]).toMatchObject({
      topic: topicName,
      partitions: [{ partition: 0, offset: 2n }],
    });

    await coordinator.send(LeaveGroup, { groupId, memberId: join.memberId });
  });

  testIfKafkaAtLeast_4_2('advertises OffsetCommit and OffsetFetch v10', () => {
    expect(seedBroker?.versions?.[API_KEYS.OffsetCommit]?.maxVersion).toBeGreaterThanOrEqual(10);
    expect(seedBroker?.versions?.[API_KEYS.OffsetFetch]?.maxVersion).toBeGreaterThanOrEqual(10);
  });

  it('lists earliest and latest offsets after a produce', async () => {
    const metadata = await retryProtocol(TRANSIENT_METADATA_ERRORS, () =>
      seedBroker!.send(Metadata, { topics: [topicName], allowAutoTopicCreation: true }),
    );
    const partition = metadata.topicMetadata[0]!.partitionMetadata[0]!;
    const brokerData = metadata.brokers.find((b) => b.nodeId === partition.leader)!;
    leader = new Broker({
      connectionPool: createConnectionPool(advertisedAddress(brokerData.host, brokerData.port)),
      logger: newLogger(),
    });
    await leader.connect();

    await retryProtocol(TRANSIENT_METADATA_ERRORS, () =>
      leader!.send(Produce, {
        acks: 1,
        timeout: 30_000,
        topicData: [
          {
            topic: topicName,
            partitions: [
              {
                partition: 0,
                messages: [
                  { key: 'a', value: '1' },
                  { key: 'b', value: '2' },
                ],
              },
            ],
          },
        ],
      }),
    );

    const earliest = await leader.send(ListOffsets, {
      topics: [{ topic: topicName, partitions: [{ partition: 0, timestamp: BigInt(EARLIEST_OFFSET) }] }],
    });
    const latest = await leader.send(ListOffsets, {
      topics: [{ topic: topicName, partitions: [{ partition: 0, timestamp: BigInt(LATEST_OFFSET) }] }],
    });
    expect(earliest.responses[0]?.partitions[0]?.offset).toBe(0n);
    expect(latest.responses[0]?.partitions[0]?.offset).toBe(2n);
  });
});
