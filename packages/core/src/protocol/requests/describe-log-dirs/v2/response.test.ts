import { describe, expect, it } from 'vitest';
import { KafkaProtocolError } from '../../../../errors';
import { Encoder } from '../../../encoder';
import { describeLogDirsResponseV2, responseSchema } from './response';

describe('protocol/requests/describe-log-dirs/v2/response', () => {
  it('round-trips a flexible v2 response', async () => {
    const value = {
      throttleTime: 0,
      logDirs: [
        {
          errorCode: 0,
          logDir: '/var/kafka/data',
          topics: [
            {
              topic: 'orders',
              partitions: [{ partition: 0, size: 1024n, offsetLag: 3n, isFuture: true }],
            },
          ],
        },
      ],
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await describeLogDirsResponseV2.decode(encoder.buffer);
    expect(data).toEqual(value);
    await expect(describeLogDirsResponseV2.parse(data)).resolves.toEqual(data);
  });

  it('throws the first log-dir error from parse', async () => {
    const value = {
      throttleTime: 0,
      logDirs: [{ errorCode: 5, logDir: '/var/kafka/offline', topics: [] }],
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await describeLogDirsResponseV2.decode(encoder.buffer);
    await expect(describeLogDirsResponseV2.parse(data)).rejects.toBeInstanceOf(KafkaProtocolError);
  });
});
