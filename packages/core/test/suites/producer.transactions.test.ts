import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createConsumer } from '../../src/consumer/index.js';
import { createProducer } from '../../src/producer/index.js';
import {
  createCluster,
  createTopic,
  newLogger,
  secureRandom,
  waitForConsumerToJoinGroup,
  waitForMessages,
} from '../helpers/index.js';

describe('producer.transactions', () => {
  let topicName: string;
  let transactionalId: string;
  let producer: ReturnType<typeof createProducer> | undefined;
  let consumer: ReturnType<typeof createConsumer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    transactionalId = `transactional-id-${secureRandom()}`;
    await createTopic({ topic: topicName });
  });

  afterEach(async () => {
    await consumer?.disconnect();
    await producer?.disconnect();
  });

  it('commits a transaction so the consumer sees the messages', async () => {
    producer = createProducer({
      cluster: createCluster(),
      logger: newLogger(),
      idempotent: true,
      transactionalId,
    });
    consumer = createConsumer({
      cluster: createCluster(),
      groupId: `group-${secureRandom()}`,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: topicName, fromBeginning: true });
    const consumed: { message: { value: Buffer | null } }[] = [];
    const join = waitForConsumerToJoinGroup(consumer);
    await consumer.run({ eachMessage: async (event) => consumed.push(event) });
    await join;

    const transaction = await producer.transaction();
    await transaction.send({ topic: topicName, messages: [{ key: 'k', value: 'committed' }] });
    await transaction.commit();
    await waitForMessages(consumed, { number: 1 });
    expect(consumed[0]?.message.value?.toString()).toBe('committed');
  });

  it('aborts a transaction so the consumer sees nothing', async () => {
    producer = createProducer({
      cluster: createCluster(),
      logger: newLogger(),
      idempotent: true,
      transactionalId,
    });
    consumer = createConsumer({
      cluster: createCluster(),
      groupId: `group-${secureRandom()}`,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: topicName, fromBeginning: true });
    const consumed: unknown[] = [];
    const join = waitForConsumerToJoinGroup(consumer);
    await consumer.run({ eachMessage: async (event) => consumed.push(event) });
    await join;

    const transaction = await producer.transaction();
    await transaction.send({ topic: topicName, messages: [{ key: 'k', value: 'aborted' }] });
    await transaction.abort();
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(consumed).toHaveLength(0);
  });

  it('cancels an in-flight transaction when a second producer uses the same id', async () => {
    const first = createProducer({
      cluster: createCluster(),
      logger: newLogger(),
      idempotent: true,
      transactionalId,
      transactionTimeout: 100,
    });
    const second = createProducer({
      cluster: createCluster(),
      logger: newLogger(),
      idempotent: true,
      transactionalId,
    });
    producer = second;
    await first.connect();
    const tx1 = await first.transaction();
    await tx1.send({ topic: topicName, messages: [{ key: 'k', value: 'v' }] });
    await second.connect();
    const tx2 = await second.transaction();
    await tx2.send({ topic: topicName, messages: [{ key: 'k2', value: 'v2' }] });
    await tx2.commit();
    await first.disconnect();
  });
});
