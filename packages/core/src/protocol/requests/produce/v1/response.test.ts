import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { produceResponseV1 } from './response';

describe('protocol/requests/produce/v1/response', () => {
  it('decodes throttleTime after the v0 topic array', async () => {
    const encoded = new Encoder()
      .writeArray([
        new Encoder().writeString('test-topic-1').writeArray([new Encoder().writeInt32(0).writeInt16(0).writeInt64(3)]),
      ])
      .writeInt32(0);

    const data = await produceResponseV1.decode(encoded.buffer);
    expect(data).toEqual({
      topics: [
        {
          topicName: 'test-topic-1',
          partitions: [{ partition: 0, errorCode: 0, baseOffset: 3n, logAppendTime: -1n, logStartOffset: -1n }],
        },
      ],
      throttleTime: 0,
    });
    await expect(produceResponseV1.parse(data)).resolves.toBeTruthy();
  });
});
