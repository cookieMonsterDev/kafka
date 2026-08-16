import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { COORDINATOR_TYPES } from '../../../src/protocol/enums/coordinator-types.js';
import { createCluster, secureRandom } from '../../helpers/index.js';

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

  it('finds the controller broker', async () => {
    const controller = await cluster!.findControllerBroker();
    expect(controller.isConnected()).toBe(true);
  });
});
