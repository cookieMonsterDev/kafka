import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createConsumer } from '../../src/consumer/index.js';
import { createProducer } from '../../src/producer/index.js';
import {
  createCluster,
  createTopic,
  newLogger,
  secureRandom,
  waitForConsumerToJoinGroup,
  waitForNextEvent,
} from '../helpers/index.js';

describe('consumer.instrumentation', () => {
  let topicName: string;
  let producer: ReturnType<typeof createProducer> | undefined;
  let consumer: ReturnType<typeof createConsumer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    await createTopic({ topic: topicName });
    producer = createProducer({ cluster: createCluster(), logger: newLogger() });
    consumer = createConsumer({
      cluster: createCluster(),
      groupId: `group-${secureRandom()}`,
      maxWaitTimeInMs: 100,
      heartbeatInterval: 100,
      logger: newLogger(),
    });
  });

  afterEach(async () => {
    await consumer?.disconnect();
    await producer?.disconnect();
  });

  it('emits connect, group join, fetch, and disconnect', async () => {
    const events: string[] = [];
    consumer!.on(consumer!.events.CONNECT, () => events.push('connect'));
    consumer!.on(consumer!.events.GROUP_JOIN, () => events.push('group_join'));
    consumer!.on(consumer!.events.FETCH, () => events.push('fetch'));
    consumer!.on(consumer!.events.DISCONNECT, () => events.push('disconnect'));

    await consumer!.connect();
    await producer!.connect();
    await consumer!.subscribe({ topic: topicName, fromBeginning: true });
    const join = waitForConsumerToJoinGroup(consumer!);
    const fetch = waitForNextEvent(consumer!, consumer!.events.FETCH);
    await consumer!.run({ eachMessage: async () => undefined });
    await join;
    await producer!.send({ acks: 1, topic: topicName, messages: [{ key: 'k', value: 'v' }] });
    await fetch;
    await consumer!.disconnect();
    consumer = undefined;

    expect(events).toEqual(expect.arrayContaining(['connect', 'group_join', 'fetch', 'disconnect']));
  });
});
