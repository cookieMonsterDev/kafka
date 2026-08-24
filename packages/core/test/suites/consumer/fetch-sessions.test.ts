import { afterEach, beforeEach, describe, expect } from 'vitest';
import { createConsumer } from '../../../src/consumer/index';
import { InstrumentationEventEmitter } from '../../../src/instrumentation/emitter';
import { createProducer } from '../../../src/producer/index';
import {
  createCluster,
  createTopic,
  newLogger,
  secureRandom,
  testIfKafkaAtLeast_2_4,
  waitFor,
  waitForConsumerToJoinGroup,
} from '../../helpers/index';

/**
 * KIP-227: once a fetch session is established, a partition with no new data since the last
 * fetch is omitted from the request entirely instead of being resent every cycle. With six
 * partitions and only one ever produced to, the first (full) Fetch request lists every
 * partition; every request after that lists none, and the wire request shrinks accordingly.
 */
describe('consumer.fetch-sessions', () => {
  let topicName: string;
  let producer: ReturnType<typeof createProducer> | undefined;
  let consumer: ReturnType<typeof createConsumer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    await createTopic({ topic: topicName, partitions: 6 });
    producer = createProducer({ cluster: createCluster(), logger: newLogger() });
  });

  afterEach(async () => {
    await consumer?.disconnect();
    await producer?.disconnect();
  });

  testIfKafkaAtLeast_2_4(
    'reuses a broker-granted session id and shrinks later Fetch requests once partitions go idle',
    async () => {
      const instrumentationEmitter = new InstrumentationEventEmitter();
      consumer = createConsumer({
        cluster: createCluster({ instrumentationEmitter }),
        groupId: `group-${secureRandom()}`,
        maxWaitTimeInMs: 100,
        heartbeatInterval: 100,
        logger: newLogger(),
        instrumentationEmitter,
      });

      const fetchRequestSizes: number[] = [];
      consumer.on(consumer.events.REQUEST, (event) => {
        const payload = event.payload as { apiName: string; size: number };
        if (payload.apiName === 'Fetch') fetchRequestSizes.push(payload.size);
      });

      await consumer.connect();
      await producer!.connect();
      await consumer.subscribe({ topic: topicName, fromBeginning: true });
      const join = waitForConsumerToJoinGroup(consumer);

      const received: unknown[] = [];
      await consumer.run({
        eachMessage: async ({ message }) => {
          received.push(message);
        },
      });
      await join;

      await producer!.send({ acks: 1, topic: topicName, messages: [{ key: 'k', value: 'v', partition: 0 }] });
      await waitFor(() => received.length > 0);

      // Give the consumer a handful more idle fetch cycles against all six (now unchanged) partitions.
      await waitFor(() => fetchRequestSizes.length >= 4, { maxWait: 10_000 });

      const [firstRequestSize] = fetchRequestSizes;
      const laterRequestSizes = fetchRequestSizes.slice(1);

      expect(firstRequestSize).toBeGreaterThan(0);
      for (const size of laterRequestSizes) {
        expect(size).toBeLessThan(firstRequestSize!);
      }
    },
  );
});
