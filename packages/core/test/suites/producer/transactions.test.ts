import { afterEach, beforeEach, describe, expect } from 'vitest';
import { createConsumer } from '../../../src/consumer/index';
import { InstrumentationEventEmitter } from '../../../src/instrumentation/emitter';
import type { InstrumentationEvent } from '../../../src/instrumentation/event';
import {
  NETWORK_REQUEST,
  type NetworkEventMap,
  type NetworkRequestEvent,
} from '../../../src/network/instrumentation-events';
import { createProducer } from '../../../src/producer/index';
import {
  createCluster,
  createTopic,
  newLogger,
  secureRandom,
  waitForConsumerToJoinGroup,
  waitForMessages,
  testIfKafkaAtLeast_0_11,
  testIfKafkaAtLeast_4_0,
  testIfKafkaTransactionV1,
} from '../../helpers/index';

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

  testIfKafkaAtLeast_0_11('commits a transaction so the consumer sees the messages', async () => {
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
    await consumer.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    await join;

    const transaction = await producer.transaction();
    await transaction.send({ topic: topicName, messages: [{ key: 'k', value: 'committed' }] });
    await transaction.commit();
    await waitForMessages(consumed, { number: 1 });
    expect(consumed[0]?.message.value?.toString()).toBe('committed');
  });

  testIfKafkaAtLeast_0_11('aborts a transaction so the consumer sees nothing', async () => {
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
    await consumer.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    await join;

    const transaction = await producer.transaction();
    await transaction.send({ topic: topicName, messages: [{ key: 'k', value: 'aborted' }] });
    await transaction.abort();
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(consumed).toHaveLength(0);
  });

  testIfKafkaAtLeast_0_11('cancels an in-flight transaction when a second producer uses the same id', async () => {
    const first = createProducer({
      cluster: createCluster(),
      logger: newLogger(),
      idempotent: true,
      transactionalId,
      transactionTimeout: 2000,
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
    expect(tx2.isActive()).toBe(true);
    await tx2.send({ topic: topicName, messages: [{ key: 'k2', value: 'v2' }] });
    await expect(tx2.commit()).resolves.toBeUndefined();
    expect(tx2.isActive()).toBe(false);
    await first.disconnect();
  });

  /** KIP-890 part 2: transaction V2 (Produce v12+, Kafka 4.0+) lets Produce itself cover AddPartitionsToTxn. */
  function countAddPartitionsToTxnRequests() {
    const instrumentationEmitter = new InstrumentationEventEmitter<NetworkEventMap>();
    const counter = { value: 0 };
    instrumentationEmitter.addListener(NETWORK_REQUEST, (event: InstrumentationEvent<NetworkRequestEvent>) => {
      if (event.payload.apiName === 'AddPartitionsToTxn') counter.value += 1;
    });
    return { counter, instrumentationEmitter };
  }

  testIfKafkaAtLeast_4_0('does not send AddPartitionsToTxn under transaction V2', async () => {
    const tracker = countAddPartitionsToTxnRequests();
    producer = createProducer({
      cluster: createCluster({ instrumentationEmitter: tracker.instrumentationEmitter }),
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
    await consumer.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    await join;

    const transaction = await producer.transaction();
    await transaction.send({ topic: topicName, messages: [{ key: 'k', value: 'v2-committed' }] });
    await transaction.commit();
    await waitForMessages(consumed, { number: 1 });

    expect(tracker.counter.value).toBe(0);
  });

  testIfKafkaTransactionV1('still sends AddPartitionsToTxn on brokers older than transaction V2', async () => {
    const tracker = countAddPartitionsToTxnRequests();
    producer = createProducer({
      cluster: createCluster({ instrumentationEmitter: tracker.instrumentationEmitter }),
      logger: newLogger(),
      idempotent: true,
      transactionalId,
    });
    await producer.connect();

    const transaction = await producer.transaction();
    await transaction.send({ topic: topicName, messages: [{ key: 'k', value: 'v1-committed' }] });
    await transaction.commit();

    expect(tracker.counter.value).toBeGreaterThan(0);
  });
});
