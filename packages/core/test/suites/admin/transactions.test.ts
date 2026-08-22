import { afterEach, beforeEach, describe, expect } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { createProducer } from '../../../src/producer/index';
import { createCluster, createTopic, newLogger, secureRandom, testIfKafkaAtLeast_3_0 } from '../../helpers/index';

describe('admin.transactions', () => {
  let topicName: string;
  let transactionalId: string;
  let admin: ReturnType<typeof createAdmin> | undefined;
  let producer: ReturnType<typeof createProducer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    transactionalId = `transactional-id-${secureRandom()}`;
    await createTopic({ topic: topicName });
  });

  afterEach(async () => {
    await producer?.disconnect();
    await admin?.disconnect();
  });

  testIfKafkaAtLeast_3_0('describes an active transaction through its coordinator', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    producer = createProducer({
      cluster: createCluster(),
      logger: newLogger(),
      idempotent: true,
      transactionalId,
    });
    await admin.connect();
    await producer.connect();

    const transaction = await producer.transaction();
    await transaction.send({ topic: topicName, messages: [{ key: 'key', value: 'value' }] });

    const { transactionStates } = await admin.describeTransactions([transactionalId]);
    expect(transactionStates).toHaveLength(1);
    expect(transactionStates[0]).toMatchObject({
      transactionalId,
      transactionState: 'Ongoing',
      topics: [{ topic: topicName, partitions: [0] }],
    });
    expect(transactionStates[0]?.producerId).toEqual(expect.any(BigInt));

    await transaction.abort();
  });

  testIfKafkaAtLeast_3_0('lists an active transaction from every coordinator', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    producer = createProducer({
      cluster: createCluster(),
      logger: newLogger(),
      idempotent: true,
      transactionalId,
    });
    await admin.connect();
    await producer.connect();

    const transaction = await producer.transaction();
    await transaction.send({ topic: topicName, messages: [{ key: 'key', value: 'value' }] });

    const { transactionStates } = await admin.listTransactions();
    const listing = transactionStates.find((state) => state.transactionalId === transactionalId);
    expect(listing).toMatchObject({
      transactionalId,
      transactionState: 'Ongoing',
    });
    expect(listing?.producerId).toEqual(expect.any(BigInt));

    await transaction.abort();
  });
});
