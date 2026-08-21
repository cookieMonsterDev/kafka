import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { renewDelegationTokenResponseV0 } from './response';

function responseFixture(errorCode = 0): Buffer {
  return new Encoder().writeInt16(errorCode).writeInt64(1_700_003_600_000n).writeInt32(4).buffer;
}

describe('protocol/requests/renew-delegation-token/v0/response', () => {
  it('decodes expiry timestamp and throttle at the end', async () => {
    await expect(renewDelegationTokenResponseV0.decode(responseFixture())).resolves.toEqual({
      errorCode: 0,
      expiryTimestampMs: 1_700_003_600_000n,
      throttleTime: 4,
    });
  });

  it('rejects a top-level protocol error', async () => {
    const data = await renewDelegationTokenResponseV0.decode(responseFixture(62));
    await expect(renewDelegationTokenResponseV0.parse(data)).rejects.toMatchObject({
      type: 'DELEGATION_TOKEN_NOT_FOUND',
    });
  });
});
