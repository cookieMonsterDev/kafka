import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { produceResponseV9 } from './response';

describe('protocol/requests/produce/v9/response', () => {
  it('decodes compact record_errors and remaps throttleTime', async () => {
    const encoded = new Encoder()
      .writeUVarIntArray([
        new Encoder()
          .writeUVarIntString('test-topic')
          .writeUVarIntArray([
            new Encoder()
              .writeInt32(1)
              .writeInt16(87)
              .writeInt64(0n)
              .writeInt64(-1n)
              .writeInt64(0n)
              .writeUVarIntArray([new Encoder().writeInt32(0).writeUVarIntString('record is invalid').writeUVarInt(0)])
              .writeUVarIntString('one or more records failed validation')
              .writeUVarInt(0),
          ])
          .writeUVarInt(0),
      ])
      .writeInt32(20)
      .writeUVarInt(0);

    const data = await produceResponseV9.decode(encoded.buffer);
    expect(data).toEqual({
      topics: [
        {
          topicName: 'test-topic',
          partitions: [
            {
              partition: 1,
              errorCode: 87,
              baseOffset: 0n,
              logAppendTime: -1n,
              logStartOffset: 0n,
              recordErrors: [{ batchIndex: 0, batchIndexErrorMessage: 'record is invalid' }],
              errorMessage: 'one or more records failed validation',
            },
          ],
        },
      ],
      throttleTime: 0,
      clientSideThrottleTime: 20,
    });
  });
});
