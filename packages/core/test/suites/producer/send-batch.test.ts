import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createProducer } from '../../../src/producer/index';
import {
  createCluster,
  createModPartitioner,
  createTopic,
  generateMessages,
  newLogger,
  secureRandom,
} from '../../helpers/index';

describe('producer.sendBatch', () => {
  let firstTopic: string;
  let secondTopic: string;
  let producer: ReturnType<typeof createProducer> | undefined;

  beforeEach(async () => {
    firstTopic = `test-topic-${secureRandom()}`;
    secondTopic = `test-topic-${secureRandom()}`;
    await createTopic({ topic: firstTopic });
    await createTopic({ topic: secondTopic });
  });

  afterEach(async () => {
    await producer?.disconnect();
  });

  it('sends records to several topics in one Produce', async () => {
    producer = createProducer({
      cluster: createCluster(),
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    });
    await producer.connect();

    const metadata = await producer.sendBatch({
      acks: 1,
      topicMessages: [
        { topic: firstTopic, messages: generateMessages({ number: 3 }) },
        { topic: secondTopic, messages: generateMessages({ number: 2 }) },
      ],
    });

    expect(metadata).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ topicName: firstTopic, errorCode: 0, baseOffset: expect.any(BigInt) }),
        expect.objectContaining({ topicName: secondTopic, errorCode: 0, baseOffset: expect.any(BigInt) }),
      ]),
    );
  });

  it('merges repeated topic entries in the same batch', async () => {
    producer = createProducer({
      cluster: createCluster(),
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    });
    await producer.connect();

    const metadata = await producer.sendBatch({
      acks: 1,
      topicMessages: [
        { topic: firstTopic, messages: generateMessages({ number: 1 }) },
        { topic: firstTopic, messages: generateMessages({ number: 1 }) },
      ],
    });

    expect(metadata).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ topicName: firstTopic, partition: 0, errorCode: 0, baseOffset: 0n }),
      ]),
    );
  });

  it('returns immediately for an empty topicMessages list', async () => {
    producer = createProducer({ cluster: createCluster(), logger: newLogger() });
    await producer.connect();
    await expect(producer.sendBatch({ acks: 1, topicMessages: [] })).resolves.toEqual([]);
  });

  it('flushes linger-buffered records on demand', async () => {
    producer = createProducer({
      cluster: createCluster(),
      createPartitioner: createModPartitioner,
      lingerMs: 60_000,
      logger: newLogger(),
    });
    await producer.connect();

    const sendPromise = producer.send({
      acks: 1,
      topic: firstTopic,
      messages: [{ key: 'k', value: 'v' }],
    });
    await Promise.resolve();
    await producer.flush();
    await expect(sendPromise).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ topicName: firstTopic, partition: 0, errorCode: 0, baseOffset: 0n }),
      ]),
    );
  });

  it('rejects a batch that includes an empty topic name', async () => {
    producer = createProducer({ cluster: createCluster(), logger: newLogger() });
    await producer.connect();
    await expect(
      producer.sendBatch({ acks: 1, topicMessages: [{ topic: '', messages: [{ value: 'v' }] }] }),
    ).rejects.toThrow('Invalid topic');
  });

  it('uses the producer-level acks default when sendBatch omits acks', async () => {
    producer = createProducer({
      cluster: createCluster(),
      createPartitioner: createModPartitioner,
      acks: 1,
      logger: newLogger(),
    });
    await producer.connect();
    await expect(
      producer.sendBatch({
        topicMessages: [{ topic: firstTopic, messages: [{ key: 'k', value: 'v' }] }],
      }),
    ).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ topicName: firstTopic, partition: 0, errorCode: 0, baseOffset: 0n }),
      ]),
    );
  });
});
