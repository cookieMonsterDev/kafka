import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createConsumer } from '../../src/consumer/index.js';
import { createCluster, createTopic, newLogger, secureRandom } from '../helpers/index.js';

describe('consumer.subscribe', () => {
  let topicName: string;
  let consumer: ReturnType<typeof createConsumer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    await createTopic({ topic: topicName });
    consumer = createConsumer({
      cluster: createCluster(),
      groupId: `group-${secureRandom()}`,
      logger: newLogger(),
    });
    await consumer.connect();
  });

  afterEach(async () => {
    await consumer?.disconnect();
  });

  it('subscribes to a topic by name', async () => {
    await consumer!.subscribe({ topic: topicName, fromBeginning: true });
  });

  it('subscribes to multiple topics', async () => {
    const other = `test-topic-${secureRandom()}`;
    await createTopic({ topic: other });
    await consumer!.subscribe({ topics: [topicName, other], fromBeginning: true });
  });

  it('subscribes with a regex', async () => {
    await consumer!.subscribe({ topic: new RegExp(`^${topicName}$`), fromBeginning: true });
  });
});
