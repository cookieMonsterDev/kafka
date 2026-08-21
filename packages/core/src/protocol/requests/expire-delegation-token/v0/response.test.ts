import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { expireDelegationTokenResponseV0 } from './response';

function responseFixture(errorCode = 0): Buffer {
  return new Encoder().writeInt16(errorCode).writeInt64(1_700_000_000_000n).writeInt32(0).buffer;
}

describe('protocol/requests/expire-delegation-token/v0/response', () => {
  it('decodes expiry timestamp and throttle at the end', async () => {
    await expect(expireDelegationTokenResponseV0.decode(responseFixture())).resolves.toEqual({
      errorCode: 0,
      expiryTimestampMs: 1_700_000_000_000n,
      throttleTime: 0,
    });
  });

  it('rejects a top-level protocol error', async () => {
    const data = await expireDelegationTokenResponseV0.decode(responseFixture(62));
    await expect(expireDelegationTokenResponseV0.parse(data)).rejects.toMatchObject({
      type: 'DELEGATION_TOKEN_NOT_FOUND',
    });
  });
});
