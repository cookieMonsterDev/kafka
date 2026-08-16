import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { addOffsetsToTxnResponseV3 } from './response';

describe('protocol/requests/add-offsets-to-txn/v3/response', () => {
  it('decodes a flexible body and remaps throttleTime', async () => {
    const data = await addOffsetsToTxnResponseV3.decode(
      new Encoder().writeInt32(6).writeInt16(0).writeUVarInt(0).buffer,
    );

    expect(data).toEqual({ throttleTime: 0, clientSideThrottleTime: 6, errorCode: 0 });
    await expect(addOffsetsToTxnResponseV3.parse(data)).resolves.toBe(data);
  });
});
