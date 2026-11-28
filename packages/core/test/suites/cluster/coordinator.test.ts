import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { COORDINATOR_TYPES } from '../../../src/protocol/enums/coordinator-types';
import { createCluster, secureRandom } from '../../helpers/index';

describe('cluster.coordinator', () => {
  let cluster: ReturnType<typeof createCluster> | undefined;
  let groupId: string;

  beforeEach(async () => {
    cluster = createCluster();
    await cluster.connect();
    await cluster.refreshMetadata();
    groupId = `test-group-${secureRandom()}`;
  });

  afterEach(async () => {
    await cluster?.disconnect();
  });

  it('finds the group coordinator', async () => {
    const broker = await cluster!.findGroupCoordinator({ groupId });
    expect(broker.isConnected()).toBe(true);
  });

  it('finds the transactional coordinator', async () => {
    const broker = await cluster!.findGroupCoordinator({
      groupId,
      coordinatorType: COORDINATOR_TYPES.TRANSACTION,
    });
    expect(broker.isConnected()).toBe(true);
  });

  it('finds the coordinators of several groups and transactional ids', async () => {
    const groupIds = [groupId, `${groupId}-2`, `${groupId}-3`];
    const groupCoordinators = await cluster!.findCoordinators({ keys: groupIds });
    const transactionCoordinators = await cluster!.findCoordinators({
      keys: groupIds,
      coordinatorType: COORDINATOR_TYPES.TRANSACTION,
    });

    expect(groupCoordinators).toHaveLength(groupIds.length);
    expect(transactionCoordinators).toHaveLength(groupIds.length);
    for (const broker of [...groupCoordinators, ...transactionCoordinators]) {
      expect(broker.isConnected()).toBe(true);
    }
    // A batched lookup agrees with looking each key up alone.
    await expect(cluster!.findGroupCoordinator({ groupId: groupIds[1]! })).resolves.toBe(groupCoordinators[1]);
  });

  it('finds the controller broker', async () => {
    const controller = await cluster!.findControllerBroker();
    expect(controller.isConnected()).toBe(true);
  });
});
