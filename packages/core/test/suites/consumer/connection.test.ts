import { afterEach, describe, expect, it } from 'vitest';
import { createConsumer } from '../../../src/consumer/index';
import { KafkaNonRetriableError } from '../../../src/errors';
import {
  createCluster,
  newLogger,
  saslBrokers,
  saslEntries,
  secureRandom,
  sslBrokers,
  sslConnectionOpts,
} from '../../helpers/index';

describe('consumer.connection', () => {
  let consumer: ReturnType<typeof createConsumer> | undefined;

  afterEach(async () => {
    await consumer?.disconnect();
  });

  it('requires a groupId', () => {
    expect(() => createConsumer({ cluster: createCluster(), groupId: '', logger: newLogger() })).toThrow(
      KafkaNonRetriableError,
    );
  });

  it('connects over PLAINTEXT', async () => {
    consumer = createConsumer({
      cluster: createCluster(),
      groupId: `group-${secureRandom()}`,
      logger: newLogger(),
    });
    await expect(consumer.connect()).resolves.toBeUndefined();
  });

  it('rejects connect when the signal is already aborted', async () => {
    consumer = createConsumer({
      cluster: createCluster(),
      groupId: `group-${secureRandom()}`,
      logger: newLogger(),
    });
    await expect(consumer.connect({ signal: AbortSignal.abort() })).rejects.toThrow(/aborted/i);
    await expect(consumer.connect()).resolves.toBeUndefined();
  });

  it('rejects run when the signal is already aborted', async () => {
    consumer = createConsumer({
      cluster: createCluster(),
      groupId: `group-${secureRandom()}`,
      logger: newLogger(),
    });
    await consumer.connect();
    await expect(consumer.run({ signal: AbortSignal.abort(), eachMessage: async () => undefined })).rejects.toThrow(
      /aborted/i,
    );
  });

  it('connects over SSL', async () => {
    consumer = createConsumer({
      cluster: createCluster(sslConnectionOpts(), sslBrokers()),
      groupId: `group-${secureRandom()}`,
      logger: newLogger(),
    });
    await expect(consumer.connect()).resolves.toBeUndefined();
  });

  it.each(saslEntries)('connects over SASL $name', async (entry) => {
    consumer = createConsumer({
      cluster: createCluster(entry.opts(), saslBrokers()),
      groupId: `group-${secureRandom()}`,
      logger: newLogger(),
    });
    await expect(consumer.connect()).resolves.toBeUndefined();
  });
});
