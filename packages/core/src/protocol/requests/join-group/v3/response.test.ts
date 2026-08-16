import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { joinGroupResponseV3 } from './response';

function buildWire(throttleTime: number): Buffer {
  return new Encoder()
    .writeInt32(throttleTime)
    .writeInt16(0) // errorCode
    .writeInt32(1) // generationId
    .writeString('proto')
    .writeString('leader')
    .writeString('member')
    .writeArray(
      [],
    ) // members
  .buffer;
}

describe('protocol/requests/join-group/v3/response', () => {
  it('decodes the v2 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await joinGroupResponseV3.decode(buildWire(42));
    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(42);
    expect(data.errorCode).toBe(0);
    await expect(joinGroupResponseV3.parse(data)).resolves.toBeTruthy();
  });
});
