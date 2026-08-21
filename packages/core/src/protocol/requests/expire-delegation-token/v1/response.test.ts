import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { expireDelegationTokenResponseV1 } from './response';

describe('protocol/requests/expire-delegation-token/v1/response', () => {
  it('remaps throttleTime to clientSideThrottleTime (KIP-219)', async () => {
    const raw = new Encoder().writeInt16(0).writeInt64(1_700_000_000_000n).writeInt32(3).buffer;
    await expect(expireDelegationTokenResponseV1.decode(raw)).resolves.toEqual({
      errorCode: 0,
      expiryTimestampMs: 1_700_000_000_000n,
      throttleTime: 0,
      clientSideThrottleTime: 3,
    });
  });
});
