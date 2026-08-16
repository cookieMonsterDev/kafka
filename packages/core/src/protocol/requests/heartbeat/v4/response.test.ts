import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { heartbeatResponseV4 } from './response';

function encodeV4Response(options: { throttleTime: number; errorCode: number }): Buffer {
  return new Encoder().writeInt32(options.throttleTime).writeInt16(options.errorCode).writeUVarInt(0).buffer;
}

describe('protocol/requests/heartbeat/v4/response', () => {
  it('decodes a compact/tagged body and remaps throttleTime', async () => {
    const data = await heartbeatResponseV4.decode(encodeV4Response({ throttleTime: 8, errorCode: 0 }));

    expect(data).toEqual({ errorCode: 0, throttleTime: 0, clientSideThrottleTime: 8 });
    await expect(heartbeatResponseV4.parse(data)).resolves.toBe(data);
  });
});
