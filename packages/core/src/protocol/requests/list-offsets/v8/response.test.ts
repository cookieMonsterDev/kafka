import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { listOffsetsResponseV6 } from '../v6/response';
import { listOffsetsResponseV8 } from './response';

describe('protocol/requests/list-offsets/v8/response', () => {
  it('re-exports the v6 decoder', () => {
    expect(listOffsetsResponseV8).toBe(listOffsetsResponseV6);
  });

  it('decodes a flexible body through the v8 alias', async () => {
    const encoded = new Encoder().writeInt32(0).writeUVarInt(1).writeUVarInt(0).buffer;

    const data = await listOffsetsResponseV8.decode(encoded);
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      responses: [],
    });
    await expect(listOffsetsResponseV8.parse(data)).resolves.toBe(data);
  });
});
