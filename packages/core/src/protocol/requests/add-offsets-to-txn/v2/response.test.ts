import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { addOffsetsToTxnResponseV2 } from './response';

describe('protocol/requests/add-offsets-to-txn/v2/response', () => {
  it('decodes the v1 wire format, remapping throttleTime', async () => {
    const data = await addOffsetsToTxnResponseV2.decode(new Encoder().writeInt32(4).writeInt16(0).buffer);
    expect(data).toEqual({ throttleTime: 0, clientSideThrottleTime: 4, errorCode: 0 });
    await expect(addOffsetsToTxnResponseV2.parse(data)).resolves.toBe(data);
  });
});
