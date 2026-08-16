import { afterEach, beforeEach, describe, expect } from 'vitest';
import { Broker } from '../../../src/broker/index';
import { COORDINATOR_TYPES } from '../../../src/protocol/enums/coordinator-types';
import {
  advertisedAddress,
  createConnectionPool,
  createTopic,
  newLogger,
  retryProtocol,
  secureRandom,
  testIfKafkaAtLeast_0_11,
} from '../../helpers/index';

describe('broker.transactions', () => {
  let transactionalId: string;
  let topicName: string;
  let seedBroker: Broker | undefined;
  let coordinator: Broker | undefined;

  beforeEach(async () => {
    transactionalId = `producer-group-id-${secureRandom()}`;
    topicName = `test-topic-${secureRandom()}`;
    seedBroker = new Broker({ connectionPool: createConnectionPool(), logger: newLogger() });
    await seedBroker.connect();
    await createTopic({ topic: topicName });

    const dest = await retryProtocol('GROUP_COORDINATOR_NOT_AVAILABLE', () =>
      seedBroker!.findGroupCoordinator({
        coordinatorKey: transactionalId,
        coordinatorType: COORDINATOR_TYPES.TRANSACTION,
      }),
    );
    coordinator = new Broker({
      connectionPool: createConnectionPool(advertisedAddress(dest.coordinator.host, dest.coordinator.port)),
      logger: newLogger(),
    });
    await coordinator.connect();
  });

  afterEach(async () => {
    await coordinator?.disconnect();
    await seedBroker?.disconnect();
  });

  testIfKafkaAtLeast_0_11('allocates a producer id and runs a transaction', async () => {
    const init = await retryProtocol(['GROUP_LOAD_IN_PROGRESS', 'NOT_COORDINATOR_FOR_GROUP'], () =>
      coordinator!.initProducerId({ transactionalId, transactionTimeout: 30_000 }),
    );
    expect(init.errorCode).toBe(0);
    expect(typeof init.producerId).toBe('bigint');
    expect(init.producerEpoch).toEqual(expect.any(Number));

    const added = await coordinator!.addPartitionsToTxn({
      transactionalId,
      producerId: init.producerId,
      producerEpoch: init.producerEpoch,
      topics: [{ topic: topicName, partitions: [0] }],
    });
    expect(added.errors[0]?.partitionErrors[0]?.errorCode).toBe(0);

    const ended = await coordinator!.endTxn({
      transactionalId,
      producerId: init.producerId,
      producerEpoch: init.producerEpoch,
      transactionResult: true,
    });
    expect(ended.errorCode).toBe(0);
  });

  testIfKafkaAtLeast_0_11('allocates a producer id without a transactional id', async () => {
    const init = await retryProtocol(['GROUP_LOAD_IN_PROGRESS', 'NOT_COORDINATOR_FOR_GROUP'], () =>
      coordinator!.initProducerId({ transactionalId: null, transactionTimeout: 30_000 }),
    );
    expect(init.errorCode).toBe(0);
    expect(typeof init.producerId).toBe('bigint');
  });
});
