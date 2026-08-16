import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { offsetCommitResponseV6 } from './response';

function encodeV5Wire(options: { throttleTime: number; topic: string; partition: number; errorCode: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeInt32(1)
    .writeString(options.topic)
    .writeInt32(1)
    .writeInt32(options.partition)
    .writeInt16(options.errorCode).buffer;
}

describe('protocol/requests/offset-commit/v6/response', () => {
  it('decodes the v5 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await offsetCommitResponseV6.decode(
      encodeV5Wire({ throttleTime: 8, topic: 'orders', partition: 0, errorCode: 0 }),
    );

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      responses: [{ topic: 'orders', partitions: [{ partition: 0, errorCode: 0 }] }],
    });
    await expect(offsetCommitResponseV6.parse(data)).resolves.toBe(data);
  });
});
