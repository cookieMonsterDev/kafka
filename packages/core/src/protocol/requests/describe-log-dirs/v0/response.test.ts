import { describe, expect, it } from 'vitest';
import { KafkaProtocolError } from '../../../../errors';
import { Encoder } from '../../../encoder';
import { describeLogDirsResponseV0, responseSchema } from './response';

describe('protocol/requests/describe-log-dirs/v0/response', () => {
  it('round-trips a successful v0 response', async () => {
    const value = {
      throttleTime: 0,
      logDirs: [
        {
          errorCode: 0,
          logDir: '/var/kafka/data',
          topics: [
            {
              topic: 'orders',
              partitions: [{ partition: 0, size: 1024n, offsetLag: 0n, isFuture: false }],
            },
          ],
        },
      ],
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await describeLogDirsResponseV0.decode(encoder.buffer);
    expect(data).toEqual(value);
    await expect(describeLogDirsResponseV0.parse(data)).resolves.toEqual(data);
  });

  it('throws the first log-dir error from parse', async () => {
    const value = {
      throttleTime: 0,
      logDirs: [{ errorCode: 5, logDir: '/var/kafka/offline', topics: [] }],
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await describeLogDirsResponseV0.decode(encoder.buffer);
    await expect(describeLogDirsResponseV0.parse(data)).rejects.toBeInstanceOf(KafkaProtocolError);
  });
});
