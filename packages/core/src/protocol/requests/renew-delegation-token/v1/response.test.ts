import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { renewDelegationTokenResponseV1 } from './response';

describe('protocol/requests/renew-delegation-token/v1/response', () => {
  it('remaps throttleTime to clientSideThrottleTime (KIP-219)', async () => {
    const raw = new Encoder().writeInt16(0).writeInt64(1_700_003_600_000n).writeInt32(4).buffer;
    await expect(renewDelegationTokenResponseV1.decode(raw)).resolves.toEqual({
      errorCode: 0,
      expiryTimestampMs: 1_700_003_600_000n,
      throttleTime: 0,
      clientSideThrottleTime: 4,
    });
  });
});
