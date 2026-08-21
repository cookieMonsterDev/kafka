import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { produceResponseV10 } from '../v10/response';
import { produceResponseV11 } from './response';

function encodeFlexibleProduceResponse(): Buffer {
  return new Encoder()
    .writeUVarIntArray([
      new Encoder()
        .writeUVarIntString('test-topic')
        .writeUVarIntArray([
          new Encoder()
            .writeInt32(1)
            .writeInt16(0)
            .writeInt64(0n)
            .writeInt64(-1n)
            .writeInt64(0n)
            .writeUVarIntArray([])
            .writeUVarIntString(null)
            .writeUVarInt(0),
        ])
        .writeUVarInt(0),
    ])
    .writeInt32(20)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/produce/v11/response', () => {
  it('decodes the same body as v10', async () => {
    const encoded = encodeFlexibleProduceResponse();
    expect(await produceResponseV11.decode(encoded)).toEqual(await produceResponseV10.decode(encoded));
  });
});
