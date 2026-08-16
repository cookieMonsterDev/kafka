import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createConsumer } from '../../../src/consumer/index';
import { createProducer } from '../../../src/producer/index';
import type { EachMessagePayload } from '../../../src/consumer/types';
import {
  createCluster,
  createTopic,
  newLogger,
  secureRandom,
  waitForConsumerToJoinGroup,
  waitForMessages,
  testIfKafkaAtLeast_0_11,
} from '../../helpers/index';

describe('consumer.transactions', () => {
  let topicName: string;
  let producer: ReturnType<typeof createProducer> | undefined;
  let consumer: ReturnType<typeof createConsumer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    await createTopic({ topic: topicName });
  });

  afterEach(async () => {
    await consumer?.disconnect();
    await producer?.disconnect();
  });

  testIfKafkaAtLeast_0_11('does not deliver aborted transactional messages under read-committed', async () => {
    producer = createProducer({
      cluster: createCluster(),
      logger: newLogger(),
      idempotent: true,
      transactionalId: `txn-${secureRandom()}`,
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
    const consumed: EachMessagePayload[] = [];
    const join = waitForConsumerToJoinGroup(consumer);
    await consumer.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    await join;

    const aborted = await producer.transaction();
    await aborted.send({ topic: topicName, messages: [{ key: 'a', value: 'aborted' }] });
    await aborted.abort();

    const committed = await producer.transaction();
    await committed.send({ topic: topicName, messages: [{ key: 'c', value: 'committed' }] });
    await committed.commit();

    await waitForMessages(consumed, { number: 1 });
    expect(consumed.map((c) => c.message.value?.toString())).toEqual(['committed']);
  });
});
