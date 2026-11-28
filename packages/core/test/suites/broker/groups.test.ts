import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Broker } from '../../../src/broker/index';
import { MemberMetadata } from '../../../src/consumer/assigner-protocol';
import { COORDINATOR_TYPES } from '../../../src/protocol/enums/coordinator-types';
import {
  advertisedAddress,
  createConnectionPool,
  newLogger,
  retryProtocol,
  secureRandom,
  testIfKafkaAtLeast_4_0,
  testIfKafkaAtMost_3_6,
} from '../../helpers/index';
import { DescribeGroups } from '../../../src/protocol/requests/describe-groups/index';
import { FindCoordinator } from '../../../src/protocol/requests/find-coordinator/index';
import { Heartbeat } from '../../../src/protocol/requests/heartbeat/index';
import { JoinGroup } from '../../../src/protocol/requests/join-group/index';
import { LeaveGroup } from '../../../src/protocol/requests/leave-group/index';
import { SyncGroup } from '../../../src/protocol/requests/sync-group/index';

describe('broker.groups', () => {
  let groupId: string;
  let topicName: string;
  let seedBroker: Broker | undefined;
  let coordinator: Broker | undefined;

  beforeEach(async () => {
    groupId = `consumer-group-id-${secureRandom()}`;
    topicName = `test-topic-${secureRandom()}`;
    seedBroker = new Broker({ connectionPool: createConnectionPool(), logger: newLogger() });
    await seedBroker.connect();
    const { coordinator: dest } = await retryProtocol('GROUP_COORDINATOR_NOT_AVAILABLE', () =>
      seedBroker!.send(FindCoordinator, {
        coordinatorKey: groupId,
        coordinatorType: COORDINATOR_TYPES.GROUP,
      }),
    );
    coordinator = new Broker({
      connectionPool: createConnectionPool(advertisedAddress(dest.host, dest.port)),
      logger: newLogger(),
    });
    await coordinator.connect();
  });

  afterEach(async () => {
    await coordinator?.disconnect();
    await seedBroker?.disconnect();
  });

  it('joins, heartbeats, syncs, and leaves a group', async () => {
    const join = await retryProtocol(['NOT_COORDINATOR_FOR_GROUP', 'GROUP_LOAD_IN_PROGRESS'], () =>
      coordinator!.send(JoinGroup, {
        groupId,
        sessionTimeout: 30_000,
        rebalanceTimeout: 60_000,
        memberId: '',
        protocolType: 'consumer',
        groupProtocols: [
          { name: 'AssignerName', metadata: MemberMetadata.encode({ version: 1, topics: [topicName] }) },
        ],
      }),
    );
    expect(join.errorCode).toBe(0);
    expect(join.memberId).toEqual(expect.any(String));
    expect(join.generationId).toEqual(expect.any(Number));

    // Heartbeat before SyncGroup is REBALANCE_IN_PROGRESS on Kafka 2.x (CompletingRebalance).
    const sync = await coordinator!.send(SyncGroup, {
      groupId,
      generationId: join.generationId,
      memberId: join.memberId,
      protocolType: 'consumer',
      protocolName: join.groupProtocol,
      groupAssignment: [{ memberId: join.memberId, memberAssignment: Buffer.alloc(0) }],
    });
    expect(sync.errorCode).toBe(0);

    const heartbeat = await coordinator!.send(Heartbeat, {
      groupId,
      groupGenerationId: join.generationId,
      memberId: join.memberId,
    });
    expect(heartbeat.errorCode).toBe(0);

    const leave = await coordinator!.send(LeaveGroup, { groupId, memberId: join.memberId });
    expect(leave.errorCode).toBe(0);
  });

  testIfKafkaAtMost_3_6('describes an unknown group as Dead', async () => {
    const described = await coordinator!.send(DescribeGroups, { groupIds: [groupId] });
    expect(described.groups[0]).toMatchObject({ groupId, state: 'Dead', errorCode: 0 });
  });

  testIfKafkaAtLeast_4_0('rejects an unknown group with GROUP_ID_NOT_FOUND on DescribeGroups v6', async () => {
    await expect(coordinator!.send(DescribeGroups, { groupIds: [groupId] })).rejects.toMatchObject({
      type: 'GROUP_ID_NOT_FOUND',
    });
  });
});
