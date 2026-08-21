import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { describeDelegationTokenResponseV1 } from './response';

describe('protocol/requests/describe-delegation-token/v1/response', () => {
  it('remaps throttleTime to clientSideThrottleTime (KIP-219)', async () => {
    const raw = new Encoder().writeInt16(0).writeInt32(0).writeInt32(8).buffer;
    await expect(describeDelegationTokenResponseV1.decode(raw)).resolves.toEqual({
      errorCode: 0,
      tokens: [],
      throttleTime: 0,
      clientSideThrottleTime: 8,
    });
  });
});
