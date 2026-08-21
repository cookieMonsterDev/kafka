import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { expireDelegationTokenResponseV2 } from './response';

describe('protocol/requests/expire-delegation-token/v2/response', () => {
  it('decodes a flexible body, remapping throttleTime', async () => {
    const raw = new Encoder().writeInt16(0).writeInt64(1_700_000_000_000n).writeInt32(3).writeUVarInt(0).buffer;
    await expect(expireDelegationTokenResponseV2.decode(raw)).resolves.toEqual({
      errorCode: 0,
      expiryTimestampMs: 1_700_000_000_000n,
      throttleTime: 0,
      clientSideThrottleTime: 3,
    });
  });
});
