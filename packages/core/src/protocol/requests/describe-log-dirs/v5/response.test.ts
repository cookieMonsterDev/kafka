import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { describeLogDirsResponseV5, responseSchema } from './response';

describe('protocol/requests/describe-log-dirs/v5/response', () => {
  it('round-trips whether each log dir is cordoned', async () => {
    const value = {
      throttleTime: 0,
      errorCode: 0,
      logDirs: [
        { errorCode: 0, logDir: '/var/kafka/a', topics: [], totalBytes: 2048n, usableBytes: 1024n, isCordoned: true },
        { errorCode: 0, logDir: '/var/kafka/b', topics: [], totalBytes: 2048n, usableBytes: 2048n, isCordoned: false },
      ],
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await describeLogDirsResponseV5.decode(encoder.buffer);
    expect(data).toEqual(value);
    await expect(describeLogDirsResponseV5.parse(data)).resolves.toEqual(value);
  });
});
