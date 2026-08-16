import { afterEach, describe, expect, it } from 'vitest';
import { createConsumer } from '../../src/consumer/index.js';
import { KafkaJSNonRetriableError } from '../../src/errors.js';
import {
  createCluster,
  newLogger,
  saslBrokers,
  saslEntries,
  secureRandom,
  sslBrokers,
  sslConnectionOpts,
} from '../helpers/index.js';

describe('consumer.connection', () => {
  let consumer: ReturnType<typeof createConsumer> | undefined;

  afterEach(async () => {
    await consumer?.disconnect();
  });

  it('requires a groupId', () => {
    expect(() => createConsumer({ cluster: createCluster(), groupId: '', logger: newLogger() })).toThrow(
      KafkaJSNonRetriableError,
    );
  });

  it('connects over PLAINTEXT', async () => {
    consumer = createConsumer({
      cluster: createCluster(),
      groupId: `group-${secureRandom()}`,
      logger: newLogger(),
    });
    await consumer.connect();
  });

  it('connects over SSL', async () => {
    consumer = createConsumer({
      cluster: createCluster(sslConnectionOpts(), sslBrokers()),
      groupId: `group-${secureRandom()}`,
      logger: newLogger(),
    });
    await consumer.connect();
  });

  it.each(saslEntries)('connects over SASL $name', async (entry) => {
    consumer = createConsumer({
      cluster: createCluster(entry.opts(), saslBrokers()),
      groupId: `group-${secureRandom()}`,
      logger: newLogger(),
    });
    await consumer.connect();
  });
});
