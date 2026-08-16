import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { describeUserScramCredentialsResponseV0 } from './response';

describe('protocol/requests/describe-user-scram-credentials/v0/response', () => {
  it('decodes results and treats wire throttle as client-side', async () => {
    const encoded = new Encoder()
      .writeInt32(7)
      .writeInt16(0)
      .writeUVarInt(0)
      .writeUVarInt(2)
      .writeUVarIntString('alice')
      .writeInt16(0)
      .writeUVarInt(0)
      .writeUVarInt(2)
      .writeInt8(1)
      .writeInt32(4096)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeUVarInt(0);

    await expect(describeUserScramCredentialsResponseV0.decode(encoded.buffer)).resolves.toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 7,
      errorCode: 0,
      errorMessage: null,
      results: [
        {
          user: 'alice',
          errorCode: 0,
          errorMessage: null,
          credentialInfos: [{ mechanism: 1, iterations: 4096 }],
        },
      ],
    });
  });
});
