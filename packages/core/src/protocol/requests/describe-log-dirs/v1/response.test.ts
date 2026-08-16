import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { responseSchema } from '../v0/response';
import { describeLogDirsResponseV1 } from './response';

describe('protocol/requests/describe-log-dirs/v1/response', () => {
  it('decodes the v0 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const wire = {
      throttleTime: 50,
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
    responseSchema.write(encoder, wire);
    const data = await describeLogDirsResponseV1.decode(encoder.buffer);
    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(50);
    expect(data.logDirs).toHaveLength(1);
    await expect(describeLogDirsResponseV1.parse(data)).resolves.toEqual(data);
  });
});
