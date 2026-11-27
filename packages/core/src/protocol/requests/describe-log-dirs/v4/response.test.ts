import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { describeLogDirsResponseV4, responseSchema } from './response';

describe('protocol/requests/describe-log-dirs/v4/response', () => {
  it('round-trips total and usable bytes per log dir', async () => {
    const value = {
      throttleTime: 0,
      errorCode: 0,
      logDirs: [
        {
          errorCode: 0,
          logDir: '/var/kafka/data',
          topics: [{ topic: 'orders', partitions: [{ partition: 0, size: 1024n, offsetLag: 0n, isFuture: false }] }],
          totalBytes: 107_374_182_400n,
          usableBytes: 53_687_091_200n,
        },
        { errorCode: 0, logDir: '/var/kafka/unknown', topics: [], totalBytes: -1n, usableBytes: -1n },
      ],
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await describeLogDirsResponseV4.decode(encoder.buffer);
    expect(data).toEqual(value);
    await expect(describeLogDirsResponseV4.parse(data)).resolves.toEqual(value);
  });
});
