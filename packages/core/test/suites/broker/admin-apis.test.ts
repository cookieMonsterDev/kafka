import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Broker } from '../../../src/broker/index';
import { CONFIG_RESOURCE_TYPES } from '../../../src/protocol/enums/config-resource-types';
import {
  advertisedAddress,
  createConnectionPool,
  newLogger,
  retryProtocol,
  secureRandom,
  testIfKafkaAtLeast_0_11,
  TRANSIENT_METADATA_ERRORS,
} from '../../helpers/index';
import { CreateTopics, DeleteTopics, DescribeConfigs } from '../../../src/protocol/requests/lazy-families';
import { Metadata } from '../../../src/protocol/requests/metadata/index';

describe('broker.adminApis', () => {
  let topicName: string;
  let broker: Broker | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    broker = new Broker({ connectionPool: createConnectionPool(), logger: newLogger() });
    await broker.connect();
  });

  afterEach(async () => {
    await broker?.disconnect();
  });

  async function connectToController(): Promise<Broker> {
    const metadata = await retryProtocol(TRANSIENT_METADATA_ERRORS, () => broker!.send(Metadata, { topics: [] }));
    const controller = metadata.brokers.find((b) => b.nodeId === metadata.controllerId);
    if (!controller) throw new Error('controller not found');
    const address = advertisedAddress(controller.host, controller.port);
    if (address.host === broker!.connectionPool.host && address.port === broker!.connectionPool.port) {
      return broker!;
    }
    const next = new Broker({
      connectionPool: createConnectionPool(address),
      logger: newLogger(),
    });
    await next.connect();
    await broker!.disconnect();
    broker = next;
    return next;
  }

  it('creates and deletes topics through the controller', async () => {
    const controller = await connectToController();
    const created = await controller.send(CreateTopics, {
      topics: [{ topic: topicName, numPartitions: 1, replicationFactor: 1 }],
    });
    expect(created.topicErrors[0]?.errorCode).toBe(0);

    const deleted = await controller.send(DeleteTopics, { topics: [topicName] });
    expect(deleted.topicErrors[0]?.errorCode).toBe(0);
  });

  testIfKafkaAtLeast_0_11('describes topic configs', async () => {
    const controller = await connectToController();
    await retryProtocol(TRANSIENT_METADATA_ERRORS, () =>
      controller.send(CreateTopics, { topics: [{ topic: topicName, numPartitions: 1, replicationFactor: 1 }] }),
    );
    const described = await retryProtocol(TRANSIENT_METADATA_ERRORS, () =>
      controller.send(DescribeConfigs, {
        resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: topicName, configNames: ['cleanup.policy'] }],
      }),
    );
    expect(described.resources[0]?.configEntries[0]?.configName).toBe('cleanup.policy');
    await controller.send(DeleteTopics, { topics: [topicName] });
  });
});
