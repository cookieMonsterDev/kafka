import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { SCRAM_MECHANISMS } from '../../../enums/scram-mechanisms';
import { alterUserScramCredentialsRequestV0, requestSchema } from './request';

describe('protocol/requests/alter-user-scram-credentials/v0/request', () => {
  it('encodes deletions and upsertions', async () => {
    const salt = Buffer.from([1, 2, 3]);
    const saltedPassword = Buffer.from([4, 5]);
    const payload = {
      deletions: [{ name: 'bob', mechanism: SCRAM_MECHANISMS.SCRAM_SHA_256 }],
      upsertions: [
        {
          name: 'alice',
          mechanism: SCRAM_MECHANISMS.SCRAM_SHA_512,
          iterations: 4096,
          salt,
          saltedPassword,
        },
      ],
    };

    const encoder = await alterUserScramCredentialsRequestV0(payload).encode();
    const expected = new Encoder()
      .writeUVarInt(2)
      .writeUVarIntString('bob')
      .writeInt8(SCRAM_MECHANISMS.SCRAM_SHA_256)
      .writeUVarInt(0)
      .writeUVarInt(2)
      .writeUVarIntString('alice')
      .writeInt8(SCRAM_MECHANISMS.SCRAM_SHA_512)
      .writeInt32(4096)
      .writeUVarIntBytes(salt)
      .writeUVarIntBytes(saltedPassword)
      .writeUVarInt(0)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });
});
