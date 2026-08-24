import { beforeEach, describe, expect, it } from 'vitest';
import {
  createKafka,
  createTopic,
  generateMessages,
  secureRandom,
  waitForConsumerToJoinGroup,
  waitForMessages,
} from '../../helpers/index';
import { KafkaNonRetriableError } from '../../../src/errors';

describe('kafka.publicApi', () => {
  let topicName: string;
  let groupId: string;
  let kafka: ReturnType<typeof createKafka>;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    groupId = `group-${secureRandom()}`;
    kafka = createKafka();
    await createTopic({ topic: topicName });
  });

  it('round-trips produce and consume through Kafka.producer and Kafka.consumer', async () => {
    const producer = kafka.producer();
    const consumer = kafka.consumer({ groupId, maxWaitTimeInMs: 100 });
    try {
      await producer.connect();
      await consumer.connect();
      await consumer.subscribe({ topic: topicName, fromBeginning: true });

      const consumed: { offset: bigint; value: string | undefined }[] = [];
      const join = waitForConsumerToJoinGroup(consumer);
      await consumer.run({
        eachMessage: async (event) => {
          consumed.push({ offset: event.message.offset, value: event.message.value?.toString() });
        },
      });
      await join;

      const messages = generateMessages({ number: 3 });
      const metadata = await producer.send({ acks: 1, topic: topicName, messages });
      expect(metadata).toEqual(
        expect.arrayContaining([expect.objectContaining({ topicName, errorCode: 0, baseOffset: expect.any(BigInt) })]),
      );

      await waitForMessages(consumed, { number: 3 });
      expect(consumed.map((entry) => entry.value)).toEqual(messages.map((message) => message.value));
    } finally {
      await consumer.disconnect();
      await producer.disconnect();
    }
  });

  it('creates topics through Kafka.admin and reports cluster metadata', async () => {
    const extraTopic = `test-topic-${secureRandom()}`;
    const admin = kafka.admin();
    try {
      await admin.connect();
      await expect(
        admin.createTopics({
          waitForLeaders: true,
          topics: [{ topic: extraTopic, numPartitions: 1, replicationFactor: 1 }],
        }),
      ).resolves.toBe(true);
      expect(await admin.listTopics()).toEqual(expect.arrayContaining([extraTopic, topicName]));

      const cluster = await admin.describeCluster();
      expect(cluster.brokers.length).toBeGreaterThan(0);
      expect(typeof admin.logger().namespace).toBe('function');
    } finally {
      await admin.deleteTopics({ topics: [extraTopic] }).catch(() => undefined);
      await admin.disconnect();
    }
  });

  it('rejects an unknown consumer event name', () => {
    const consumer = kafka.consumer({ groupId });
    expect(() => consumer.on('not-an-event' as never, () => undefined)).toThrow(KafkaNonRetriableError);
  });

  it('exposes a client logger', () => {
    expect(typeof kafka.logger().info).toBe('function');
  });

  it('reports a non-idempotent producer by default', () => {
    const producer = kafka.producer();
    expect(producer.isIdempotent()).toBe(false);
  });

  it('rejects producer connect when the signal is already aborted', async () => {
    const producer = kafka.producer();
    await expect(producer.connect({ signal: AbortSignal.abort() })).rejects.toThrow(/aborted/i);
  });
});
