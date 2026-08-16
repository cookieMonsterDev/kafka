import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { offsetCommitResponseV8 } from './response';

function encodeV8Response(options: {
  throttleTime: number;
  topic: string;
  partition: number;
  errorCode: number;
}): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeUVarInt(2)
    .writeUVarIntString(options.topic)
    .writeUVarInt(2)
    .writeInt32(options.partition)
    .writeInt16(options.errorCode)
    .writeUVarInt(0)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/offset-commit/v8/response', () => {
  it('decodes a flexible body and remaps throttleTime', async () => {
    const data = await offsetCommitResponseV8.decode(
      encodeV8Response({ throttleTime: 8, topic: 'orders', partition: 0, errorCode: 0 }),
    );

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      responses: [{ topic: 'orders', partitions: [{ partition: 0, errorCode: 0 }] }],
    });
    await expect(offsetCommitResponseV8.parse(data)).resolves.toBe(data);
  });
});
