import { afterEach, beforeEach, describe, expect } from 'vitest';
import { createKafka, testIfKafkaAtLeast_3_6 } from '../../helpers/index';
import { sleep } from '../../../src/utils/wait';

describe('client telemetry (KIP-714)', () => {
  let kafka: ReturnType<typeof createKafka>;
  let producer: ReturnType<ReturnType<typeof createKafka>['producer']>;

  beforeEach(() => {
    kafka = createKafka();
    producer = kafka.producer();
  });

  afterEach(async () => {
    await producer.disconnect().catch(() => undefined);
  });

  testIfKafkaAtLeast_3_6(
    'connects without assigning a clientInstanceId when the broker does not advertise telemetry APIs',
    async () => {
      await producer.connect();
      // KIP-714: brokers only advertise GetTelemetrySubscriptions / PushTelemetry when a
      // MetricsReporter implementing ClientTelemetry is configured. The test cluster does
      // not, so the reporter disables itself on the first tick and never assigns an id.
      await sleep(250);
      expect(producer.clientInstanceId()).toBeNull();
    },
  );
});
