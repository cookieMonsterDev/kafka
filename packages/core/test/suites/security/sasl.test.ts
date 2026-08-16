import { expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Kafka } from '../../../src/client';
import { LOG_LEVELS } from '../../../src/loggers/index';
import { FAST_RETRY_DEFAULTS } from '../../../src/retry/test-defaults';
import {
  describeIfOauthbearerDisabled,
  saslBrokers,
  saslEntries,
  secureRandom,
  waitForConsumerToJoinGroup,
} from '../../helpers/index';

const certSigned = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../assets/certs/cert-signed');

describeIfOauthbearerDisabled('security.sasl', () => {
  it.each(saslEntries)('produces and consumes over SASL $name', async (entry) => {
    const sasl = entry.opts().sasl;
    const kafka = new Kafka({
      clientId: `test-${secureRandom()}`,
      brokers: saslBrokers(),
      ssl: {
        servername: 'localhost',
        rejectUnauthorized: false,
        ca: [readFileSync(certSigned, 'utf8')],
      },
      sasl: sasl as never,
      logLevel: LOG_LEVELS.NOTHING,
      retry: FAST_RETRY_DEFAULTS,
    });

    const topic = `test-topic-${secureRandom()}`;
    const admin = kafka.admin();
    const producer = kafka.producer();
    const consumer = kafka.consumer({ groupId: `group-${secureRandom()}` });

    try {
      await admin.connect();
      await admin.createTopics({ waitForLeaders: true, topics: [{ topic, numPartitions: 1, replicationFactor: 1 }] });
      await producer.connect();
      await consumer.connect();
      await consumer.subscribe({ topic, fromBeginning: true });

      const join = waitForConsumerToJoinGroup(consumer);
      const received = new Promise<string>((resolve) => {
        void consumer
          .run({
            eachMessage: async ({ message }) => {
              resolve(message.value?.toString() ?? '');
            },
          })
          .catch(() => undefined);
      });
      await join;
      await producer.send({ acks: 1, topic, messages: [{ key: 'k', value: entry.name }] });
      await expect(received).resolves.toBe(entry.name);
    } finally {
      await consumer.disconnect();
      await producer.disconnect();
      await admin.disconnect();
    }
  });
});
