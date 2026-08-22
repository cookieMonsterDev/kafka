import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { describeDelegationTokenResponseV3 } from './response';

const hmac = Buffer.from([1, 2, 3]);

function responseFixture(errorCode = 0): Buffer {
  return new Encoder()
    .writeInt16(errorCode)
    .writeUVarInt(2)
    .writeUVarIntString('User')
    .writeUVarIntString('alice')
    .writeUVarIntString('User')
    .writeUVarIntString('admin')
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

describe('protocol/requests/describe-delegation-token/v3/response', () => {
  it('decodes token requester details on each token', async () => {
    await expect(describeDelegationTokenResponseV3.decode(responseFixture())).resolves.toEqual({
      errorCode: 0,
      tokens: [
        {
          principalType: 'User',
          principalName: 'alice',
          tokenRequesterPrincipalType: 'User',
          tokenRequesterPrincipalName: 'admin',
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
});
