import { describe, expect, it } from 'vitest';
import { KafkaProtocolError } from '../../../../errors';
import { Encoder } from '../../../encoder';
import { describeLogDirsResponseV3, responseSchema } from './response';

const logDir = {
  errorCode: 0,
  logDir: '/var/kafka/data',
  topics: [{ topic: 'orders', partitions: [{ partition: 0, size: 1024n, offsetLag: 3n, isFuture: false }] }],
};

async function decode(value: Parameters<typeof responseSchema.write>[1]) {
  const encoder = new Encoder();
  responseSchema.write(encoder, value);
  return describeLogDirsResponseV3.decode(encoder.buffer);
}

describe('protocol/requests/describe-log-dirs/v3/response', () => {
  it('round-trips the top-level error code', async () => {
    const value = { throttleTime: 0, errorCode: 0, logDirs: [logDir] };

    const data = await decode(value);
    expect(data).toEqual(value);
    await expect(describeLogDirsResponseV3.parse(data)).resolves.toEqual(value);
  });

  it('throws the top-level error from parse', async () => {
    // CLUSTER_AUTHORIZATION_FAILED; the broker sends no log dirs with it.
    const data = await decode({ throttleTime: 0, errorCode: 31, logDirs: [] });
    await expect(describeLogDirsResponseV3.parse(data)).rejects.toMatchObject({ type: 'CLUSTER_AUTHORIZATION_FAILED' });
  });

  it('still throws a log-dir error from parse', async () => {
    const data = await decode({ throttleTime: 0, errorCode: 0, logDirs: [{ ...logDir, errorCode: 56 }] });
    await expect(describeLogDirsResponseV3.parse(data)).rejects.toBeInstanceOf(KafkaProtocolError);
  });
});
