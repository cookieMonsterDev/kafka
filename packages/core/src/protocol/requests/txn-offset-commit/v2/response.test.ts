import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { txnOffsetCommitResponseV2 } from './response';

describe('protocol/requests/txn-offset-commit/v2/response', () => {
  it('decodes the v1 wire format, remapping throttleTime', async () => {
    const data = await txnOffsetCommitResponseV2.decode(new Encoder().writeInt32(4).writeInt32(0).buffer);
    expect(data).toEqual({ throttleTime: 0, clientSideThrottleTime: 4, topics: [] });
    await expect(txnOffsetCommitResponseV2.parse(data)).resolves.toBe(data);
  });
});
