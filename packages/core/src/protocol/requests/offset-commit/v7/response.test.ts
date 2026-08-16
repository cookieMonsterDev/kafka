import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { offsetCommitResponseV7 } from './response';

function encodeV5Wire(options: { throttleTime: number; topic: string; partition: number; errorCode: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeInt32(1)
    .writeString(options.topic)
    .writeInt32(1)
    .writeInt32(options.partition)
    .writeInt16(options.errorCode).buffer;
}

describe('protocol/requests/offset-commit/v7/response', () => {
  it('decodes the v5/v6 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await offsetCommitResponseV7.decode(
      encodeV5Wire({ throttleTime: 4, topic: 'orders', partition: 1, errorCode: 0 }),
    );

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 4,
      responses: [{ topic: 'orders', partitions: [{ partition: 1, errorCode: 0 }] }],
    });
    await expect(offsetCommitResponseV7.parse(data)).resolves.toBe(data);
  });
});
