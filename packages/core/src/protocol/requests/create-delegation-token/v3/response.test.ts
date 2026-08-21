import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { createDelegationTokenResponseV3 } from './response';

const hmac = Buffer.from([1, 2, 3, 4]);

function responseFixture(errorCode = 0): Buffer {
  return new Encoder()
    .writeInt16(errorCode)
    .writeUVarIntString('User')
    .writeUVarIntString('alice')
    .writeUVarIntString('User')
    .writeUVarIntString('admin')
    .writeInt64(1_700_000_000_000n)
    .writeInt64(1_700_003_600_000n)
    .writeInt64(1_700_007_200_000n)
    .writeUVarIntString('token-id')
    .writeUVarIntBytes(hmac)
    .writeInt32(12)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/create-delegation-token/v3/response', () => {
  it('decodes owner, requester, timestamps, token id, and hmac', async () => {
    await expect(createDelegationTokenResponseV3.decode(responseFixture())).resolves.toEqual({
      errorCode: 0,
      principalType: 'User',
      principalName: 'alice',
      tokenRequesterPrincipalType: 'User',
      tokenRequesterPrincipalName: 'admin',
      issueTimestampMs: 1_700_000_000_000n,
      expiryTimestampMs: 1_700_003_600_000n,
      maxTimestampMs: 1_700_007_200_000n,
      tokenId: 'token-id',
      hmac,
      throttleTime: 0,
      clientSideThrottleTime: 12,
    });
  });

  it('rejects a top-level protocol error', async () => {
    const data = await createDelegationTokenResponseV3.decode(responseFixture(61));
    await expect(createDelegationTokenResponseV3.parse(data)).rejects.toMatchObject({
      type: 'DELEGATION_TOKEN_AUTH_DISABLED',
    });
  });
});
