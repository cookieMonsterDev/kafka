import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { produceResponseV0 } from './response';

describe('protocol/requests/produce/v0/response', () => {
  it('decodes offsets as bigint baseOffset and fills later fields', async () => {
    const encoded = new Encoder().writeArray([
      new Encoder()
        .writeString('test-topic-1')
        .writeArray([
          new Encoder().writeInt32(0).writeInt16(0).writeInt64(16),
          new Encoder().writeInt32(1).writeInt16(0).writeInt64(2),
        ]),
      new Encoder().writeString('test-topic-2').writeArray([new Encoder().writeInt32(4).writeInt16(0).writeInt64(11)]),
    ]);

    const data = await produceResponseV0.decode(encoded.buffer);
    expect(data).toEqual({
      topics: [
        {
          topicName: 'test-topic-1',
          partitions: [
            { partition: 0, errorCode: 0, baseOffset: 16n, logAppendTime: -1n, logStartOffset: -1n },
            { partition: 1, errorCode: 0, baseOffset: 2n, logAppendTime: -1n, logStartOffset: -1n },
          ],
        },
        {
          topicName: 'test-topic-2',
          partitions: [{ partition: 4, errorCode: 0, baseOffset: 11n, logAppendTime: -1n, logStartOffset: -1n }],
        },
      ],
      throttleTime: 0,
    });
    await expect(produceResponseV0.parse(data)).resolves.toBeTruthy();
  });
});
