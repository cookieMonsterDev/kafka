import { afterEach, describe, expect } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { createProducer } from '../../../src/producer/index';
import { createCluster, createTopic, newLogger, secureRandom, testIfKafkaAtLeast_3_0 } from '../../helpers/index';

describe('admin.producers', () => {
  let topicName: string | undefined;
  let admin: ReturnType<typeof createAdmin> | undefined;
  let producer: ReturnType<typeof createProducer> | undefined;

  afterEach(async () => {
    await producer?.disconnect();
    if (admin) {
      if (topicName) await admin.deleteTopics({ topics: [topicName] }).catch(() => undefined);
      await admin.disconnect();
    }
  });

  testIfKafkaAtLeast_3_0('describes active idempotent producers on a partition leader', async () => {
    topicName = `describe-producers-${secureRandom()}`;
    await createTopic({ topic: topicName, partitions: 1 });

    producer = createProducer({ cluster: createCluster(), logger: newLogger(), idempotent: true });
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await producer.connect();
    await admin.connect();
    await producer.send({ acks: -1, topic: topicName, messages: [{ value: 'value' }] });

    const result = await admin.describeProducers({
      topicPartitions: [{ topic: topicName, partitions: [0] }],
    });

    expect(result).toEqual([
      {
        topic: topicName,
        partition: 0,
        activeProducers: [
          expect.objectContaining({
            producerId: expect.any(BigInt),
            producerEpoch: expect.any(Number),
            lastSequence: expect.any(Number),
            lastTimestamp: expect.any(BigInt),
          }),
        ],
      },
    ]);
  });
});
