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

  testIfKafkaAtLeast_3_0('fences an active transactional producer', async () => {
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
    await transaction.send({ topic: topicName, messages: [{ key: 'k', value: 'v' }] });

    const { results } = await admin.fenceProducers({ transactionalIds: [transactionalId] });
    expect(results).toHaveLength(1);
    expect(results[0]?.transactionalId).toBe(transactionalId);
    expect(results[0]?.errorCode).toBe(0);
    expect(results[0]?.producerId).toEqual(expect.any(BigInt));

    await transaction.abort().catch(() => undefined);
  });

  testIfKafkaAtLeast_3_0('aborts an active transaction via WriteTxnMarkers on the partition leader', async () => {
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
    await transaction.send({ topic: topicName, messages: [{ key: 'k', value: 'v' }] });

    const { transactionStates } = await admin.describeTransactions([transactionalId]);
    const state = transactionStates[0];
    expect(state?.transactionState).toBe('Ongoing');
    expect(state?.topics[0]?.topic).toBe(topicName);
    expect(state?.topics[0]?.partitions).toContain(0);

    await admin.abortTransaction({
      topic: topicName,
      partition: 0,
      producerId: state!.producerId,
      producerEpoch: state!.producerEpoch,
    });

    const producerStates = await admin.describeProducers({
      topicPartitions: [{ topic: topicName, partitions: [0] }],
    });
    const partitionState = producerStates[0];
    const activeProducer = partitionState?.activeProducers.find(({ producerId }) => producerId === state!.producerId);
    expect(activeProducer?.currentTransactionStartOffset).toBeNull();

    await transaction.abort().catch(() => undefined);
  });

  testIfKafkaAtLeast_3_0('force-terminates an active transactional producer', async () => {
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
    await transaction.send({ topic: topicName, messages: [{ key: 'k', value: 'v' }] });

    const result = await admin.forceTerminateTransaction({ transactionalId });
    expect(result.transactionalId).toBe(transactionalId);
    expect(result.errorCode).toBe(0);
    expect(result.producerId).toEqual(expect.any(BigInt));

    await transaction.abort().catch(() => undefined);
  });
});
