import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { unregisterControllerRequestV0, requestSchema } from './request';

describe('protocol/requests/unregister-controller/v0/request', () => {
  it('encodes the controller id followed by an empty tag buffer', async () => {
    const encoder = await unregisterControllerRequestV0({ controllerId: 3001 }).encode();
    expect(encoder.buffer).toEqual(Buffer.from([0x00, 0x00, 0x0b, 0xb9, 0x00]));
  });

  it('round-trips a flexible v0 request', async () => {
    const value = { controllerId: 3 };

    const encoder = await unregisterControllerRequestV0(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
