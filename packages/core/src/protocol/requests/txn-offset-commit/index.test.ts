import { describe, expect, it } from 'vitest';
import { TxnOffsetCommit } from './index';

const options = {
  transactionalId: 'txn-1',
  groupId: 'g1',
  producerId: 1001n,
  producerEpoch: 0,
  topics: [{ topic: 'orders', partitions: [{ partition: 0, offset: 5n, metadata: null }] }],
};

describe('protocol/requests/txn-offset-commit', () => {
  it('implements versions 0 through 5', () => {
    expect(TxnOffsetCommit.versions).toEqual([0, 1, 2, 3, 4, 5]);
  });

  it('sends v5 only for a transaction of transaction version 2', () => {
    const advertised = { minVersion: 0, maxVersion: 5 };

    expect(TxnOffsetCommit.preferVersion?.({ ...options, isTransactionV2Enabled: true }, 5, advertised)).toBe(5);
    expect(TxnOffsetCommit.preferVersion?.({ ...options, isTransactionV2Enabled: false }, 5, advertised)).toBe(4);
    expect(TxnOffsetCommit.preferVersion?.(options, 5, advertised)).toBe(4);
    expect(TxnOffsetCommit.preferVersion?.(options, 3, { minVersion: 0, maxVersion: 3 })).toBe(3);
  });

  it('sends the same body from v3 through v5', async () => {
    const encoded = await Promise.all(
      [3, 4, 5].map(async (version) => (await TxnOffsetCommit.protocol({ version })(options).request.encode()).buffer),
    );
    expect(encoded[1]).toEqual(encoded[0]);
    expect(encoded[2]).toEqual(encoded[0]);
  });
});
