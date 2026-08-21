import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { describeDelegationTokenResponseV2 } from './response';

const hmac = Buffer.from([1, 2, 3]);

function responseFixture(errorCode = 0): Buffer {
  return new Encoder()
    .writeInt16(errorCode)
    .writeUVarInt(2)
    .writeUVarIntString('User')
    .writeUVarIntString('alice')
    .writeInt64(1_700_000_000_000n)
    .writeInt64(1_700_003_600_000n)
    .writeInt64(1_700_007_200_000n)
    .writeUVarIntString('token-id')
    .writeUVarIntBytes(hmac)
    .writeUVarInt(2)
    .writeUVarIntString('User')
    .writeUVarIntString('bob')
    .writeUVarInt(0)
    .writeUVarInt(0)
    .writeInt32(8)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/describe-delegation-token/v2/response', () => {
  it('decodes a flexible body, remapping throttleTime', async () => {
    await expect(describeDelegationTokenResponseV2.decode(responseFixture())).resolves.toEqual({
      errorCode: 0,
      tokens: [
        {
          principalType: 'User',
          principalName: 'alice',
          issueTimestamp: 1_700_000_000_000n,
          expiryTimestamp: 1_700_003_600_000n,
          maxTimestamp: 1_700_007_200_000n,
          tokenId: 'token-id',
          hmac,
          renewers: [{ principalType: 'User', name: 'bob' }],
        },
      ],
      throttleTime: 0,
      clientSideThrottleTime: 8,
    });
  });

  it('rejects a top-level protocol error', async () => {
    const data = await describeDelegationTokenResponseV2.decode(responseFixture(61));
    await expect(describeDelegationTokenResponseV2.parse(data)).rejects.toMatchObject({
      type: 'DELEGATION_TOKEN_AUTH_DISABLED',
    });
  });
});
