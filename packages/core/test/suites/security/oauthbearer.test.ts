import { expect, it } from 'vitest';
import { Kafka } from '../../../src/client.js';
import { LOG_LEVELS } from '../../../src/loggers/index.js';
import { FAST_RETRY_DEFAULTS } from '../../../src/retry/test-defaults.js';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createUnsignedJwt, describeIfOauthbearerEnabled, saslBrokers, secureRandom } from '../../helpers/index.js';

const certSigned = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../assets/certs/cert-signed');

describeIfOauthbearerEnabled('security.oauthbearer', () => {
  it('connects with an unsecured OAUTHBEARER token', async () => {
    const kafka = new Kafka({
      clientId: `test-${secureRandom()}`,
      brokers: saslBrokers(),
      ssl: {
        servername: 'localhost',
        rejectUnauthorized: false,
        ca: [readFileSync(certSigned, 'utf8')],
      },
      sasl: {
        mechanism: 'oauthbearer',
        oauthBearerProvider: () => Promise.resolve({ value: createUnsignedJwt({ sub: 'test' }) }),
      },
      logLevel: LOG_LEVELS.NOTHING,
      retry: FAST_RETRY_DEFAULTS,
    });

    const admin = kafka.admin();
    try {
      await admin.connect();
      const cluster = await admin.describeCluster();
      expect(cluster.brokers.length).toBe(3);
    } finally {
      await admin.disconnect();
    }
  });
});
