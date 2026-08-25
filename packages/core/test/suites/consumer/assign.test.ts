import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createConsumer } from '../../../src/consumer/index';
import { InstrumentationEventEmitter } from '../../../src/instrumentation/emitter';
import { createProducer } from '../../../src/producer/index';
import type { EachMessagePayload } from '../../../src/consumer/types';
import {
  createCluster,
  createModPartitioner,
  createTopic,
  generateMessages,
  newLogger,
  secureRandom,
  waitForMessages,
} from '../../helpers/index';

const GROUP_MEMBERSHIP_APIS = new Set(['JoinGroup', 'SyncGroup', 'Heartbeat', 'ConsumerGroupHeartbeat']);

describe('consumer.assign', () => {
  let topicName: string;
  let producer: ReturnType<typeof createProducer> | undefined;
  let consumer: ReturnType<typeof createConsumer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    await createTopic({ topic: topicName, partitions: 1 });
    producer = createProducer({
      cluster: createCluster(),
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    });
  });

  afterEach(async () => {
    await consumer?.disconnect();
    await producer?.disconnect();
  });

  it('consumes assigned partitions without joining a group', async () => {
    const instrumentationEmitter = new InstrumentationEventEmitter();
    consumer = createConsumer({
      cluster: createCluster({ instrumentationEmitter }),
      maxWaitTimeInMs: 100,
      logger: newLogger(),
      instrumentationEmitter,
      autoOffsetReset: 'earliest',
    });

    const apiNames: string[] = [];
    consumer.on(consumer.events.REQUEST, (event) => {
      const payload = event.payload as { apiName: string };
      apiNames.push(payload.apiName);
    });

    await producer!.connect();
    const messages = generateMessages({ number: 3 });
    await producer!.send({ acks: 1, topic: topicName, messages });

    await consumer.connect();
    await consumer.assign([{ topic: topicName, partition: 0 }]);
    const consumed: EachMessagePayload[] = [];
    await consumer.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });

    await waitForMessages(consumed, { number: 3 });

    expect(consumed.map((entry) => entry.message.value?.toString())).toEqual(messages.map((message) => message.value));
    expect(apiNames.filter((apiName) => GROUP_MEMBERSHIP_APIS.has(apiName))).toEqual([]);
    expect(apiNames).toContain('Fetch');
  });

  it('tracks position and currentLag in assign mode', async () => {
    consumer = createConsumer({
      cluster: createCluster(),
      maxWaitTimeInMs: 100,
      logger: newLogger(),
      autoOffsetReset: 'earliest',
    });

    await producer!.connect();
    await consumer.connect();
    await consumer.assign([{ topic: topicName, partition: 0 }]);

    const consumed: EachMessagePayload[] = [];
    await consumer.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });

    await producer!.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 5 }) });
    await waitForMessages(consumed, { number: 5 });

    expect(consumer.position({ topic: topicName, partition: 0 })).toBe(5n);
    expect(consumer.currentLag({ topic: topicName, partition: 0 })).toBe(0n);
    expect(consumer.position({ topic: topicName, partition: 1 })).toBeNull();
  });
});
