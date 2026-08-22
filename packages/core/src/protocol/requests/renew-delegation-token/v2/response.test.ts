import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { renewDelegationTokenResponseV2 } from './response';

describe('protocol/requests/renew-delegation-token/v2/response', () => {
  it('decodes a flexible body, remapping throttleTime', async () => {
    const raw = new Encoder().writeInt16(0).writeInt64(1_700_003_600_000n).writeInt32(4).writeUVarInt(0).buffer;
    await expect(renewDelegationTokenResponseV2.decode(raw)).resolves.toEqual({
      errorCode: 0,
      expiryTimestampMs: 1_700_003_600_000n,
      throttleTime: 0,
      clientSideThrottleTime: 4,
    });
  });
});
