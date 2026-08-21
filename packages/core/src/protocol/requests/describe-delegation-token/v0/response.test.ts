import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { describeDelegationTokenResponseV0 } from './response';

const hmac = Buffer.from([1, 2, 3]);

function responseFixture(errorCode = 0): Buffer {
  return new Encoder()
    .writeInt16(errorCode)
    .writeInt32(1)
    .writeString('User')
    .writeString('alice')
    .writeInt64(1_700_000_000_000n)
    .writeInt64(1_700_003_600_000n)
    .writeInt64(1_700_007_200_000n)
    .writeString('token-id')
    .writeBytes(hmac)
    .writeInt32(1)
    .writeString('User')
    .writeString('bob')
    .writeInt32(8).buffer;
}

describe('protocol/requests/describe-delegation-token/v0/response', () => {
  it('decodes tokens, hmac, timestamps, and renewers', async () => {
    await expect(describeDelegationTokenResponseV0.decode(responseFixture())).resolves.toEqual({
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
      throttleTime: 8,
    });
  });

  it('rejects a top-level protocol error', async () => {
    const data = await describeDelegationTokenResponseV0.decode(responseFixture(61));
    await expect(describeDelegationTokenResponseV0.parse(data)).rejects.toMatchObject({
      type: 'DELEGATION_TOKEN_AUTH_DISABLED',
    });
  });
});
