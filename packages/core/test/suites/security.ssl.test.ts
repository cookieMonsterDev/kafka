import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Kafka } from '../../src/client.js';
import { LOG_LEVELS } from '../../src/loggers/index.js';
import { FAST_RETRY_DEFAULTS } from '../../src/retry/test-defaults.js';
import { secureRandom, sslBrokers, waitForConsumerToJoinGroup } from '../helpers/index.js';

const certSigned = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../assets/certs/cert-signed');

describe('security.ssl', () => {
  it('produces and consumes over SSL', async () => {
    const kafka = new Kafka({
      clientId: `test-${secureRandom()}`,
      brokers: sslBrokers(),
      ssl: {
        servername: 'localhost',
        rejectUnauthorized: false,
        ca: [readFileSync(certSigned, 'utf8')],
      },
      logLevel: LOG_LEVELS.NOTHING,
      retry: FAST_RETRY_DEFAULTS,
    });

    const topic = `test-topic-${secureRandom()}`;
    const admin = kafka.admin();
    const producer = kafka.producer();
    const consumer = kafka.consumer({ groupId: `group-${secureRandom()}` });

    try {
      await admin.connect();
      await admin.createTopics({ waitForLeaders: true, topics: [{ topic }] });
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
      await producer.send({ acks: 1, topic, messages: [{ key: 'k', value: 'ssl' }] });
      await expect(received).resolves.toBe('ssl');
    } finally {
      await consumer.disconnect();
      await producer.disconnect();
      await admin.disconnect();
    }
  });
});
