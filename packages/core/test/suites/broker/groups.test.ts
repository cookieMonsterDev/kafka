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
} from '../../helpers/index';

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
      seedBroker!.findGroupCoordinator({
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
      coordinator!.joinGroup({
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

    const heartbeat = await coordinator!.heartbeat({
      groupId,
      groupGenerationId: join.generationId,
      memberId: join.memberId,
    });
    expect(heartbeat.errorCode).toBe(0);

    const sync = await coordinator!.syncGroup({
      groupId,
      generationId: join.generationId,
      memberId: join.memberId,
      groupAssignment: [{ memberId: join.memberId, memberAssignment: Buffer.alloc(0) }],
    });
    expect(sync.errorCode).toBe(0);

    const leave = await coordinator!.leaveGroup({ groupId, memberId: join.memberId });
    expect(leave.errorCode).toBe(0);
  });

  it('describes groups', async () => {
    const described = await coordinator!.describeGroups({ groupIds: [groupId] });
    expect(described.groups[0]?.groupId).toBe(groupId);
  });
});
