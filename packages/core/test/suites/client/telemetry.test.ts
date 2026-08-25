import { afterEach, beforeEach, describe, expect } from 'vitest';
import { createKafka, testIfKafkaAtLeast_3_6, waitFor } from '../../helpers/index';

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

  testIfKafkaAtLeast_3_6('assigns a clientInstanceId after connect', async () => {
    await producer.connect();
    const id = await waitFor(() => producer.clientInstanceId() ?? false, {
      maxWait: 8_000,
      timeoutMessage: 'broker did not assign a telemetry clientInstanceId',
    });
    expect(id).toBeInstanceOf(Buffer);
    expect(id.length).toBe(16);
    expect(id.equals(Buffer.alloc(16))).toBe(false);
  });
});
