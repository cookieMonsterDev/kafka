import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { addPartitionsToTxnResponseV2 } from './response';

function encodeV2Response(options: { throttleTime: number }): Buffer {
  return new Encoder().writeInt32(options.throttleTime).writeInt32(0).buffer;
}

describe('protocol/requests/add-partitions-to-txn/v2/response', () => {
  it('decodes the v1 wire format, remapping throttleTime', async () => {
    const data = await addPartitionsToTxnResponseV2.decode(encodeV2Response({ throttleTime: 4 }));
    expect(data).toEqual({ throttleTime: 0, clientSideThrottleTime: 4, errors: [] });
    await expect(addPartitionsToTxnResponseV2.parse(data)).resolves.toBe(data);
  });
});
