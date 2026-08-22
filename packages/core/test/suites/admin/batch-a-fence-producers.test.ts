import { afterEach, beforeEach, describe, expect } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { createProducer } from '../../../src/producer/index';
import { createCluster, createTopic, newLogger, secureRandom, testIfKafkaAtLeast_3_0 } from '../../helpers/index';

describe('admin.batch-a fenceProducers', () => {
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
});
