import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { createDelegationTokenResponseV1 } from './response';

const hmac = Buffer.from([1, 2, 3, 4]);

function responseFixture(errorCode = 0): Buffer {
  return new Encoder()
    .writeInt16(errorCode)
    .writeString('User')
    .writeString('alice')
    .writeInt64(1_700_000_000_000n)
    .writeInt64(1_700_003_600_000n)
    .writeInt64(1_700_007_200_000n)
    .writeString('token-id')
    .writeBytes(hmac)
    .writeInt32(12).buffer;
}

describe('protocol/requests/create-delegation-token/v1/response', () => {
  it('remaps throttleTime to clientSideThrottleTime (KIP-219)', async () => {
    await expect(createDelegationTokenResponseV1.decode(responseFixture())).resolves.toEqual({
      errorCode: 0,
      principalType: 'User',
      principalName: 'alice',
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
    const data = await createDelegationTokenResponseV1.decode(responseFixture(61));
    await expect(createDelegationTokenResponseV1.parse(data)).rejects.toMatchObject({
      type: 'DELEGATION_TOKEN_AUTH_DISABLED',
    });
  });
});
