import { describe, expect, it } from 'vitest';
import { EndTxn } from './index';

const options = { transactionalId: 'txn-1', producerId: 1001n, producerEpoch: 0, transactionResult: true };

describe('protocol/requests/end-txn', () => {
  it('implements versions 0 through 5', () => {
    expect(EndTxn.versions).toEqual([0, 1, 2, 3, 4, 5]);
  });

  it('sends v5 only for a transaction of transaction version 2', () => {
    const advertised = { minVersion: 0, maxVersion: 5 };

    expect(EndTxn.preferVersion?.({ ...options, isTransactionV2Enabled: true }, 5, advertised)).toBe(5);
    expect(EndTxn.preferVersion?.({ ...options, isTransactionV2Enabled: false }, 5, advertised)).toBe(4);
    expect(EndTxn.preferVersion?.(options, 5, advertised)).toBe(4);
    expect(EndTxn.preferVersion?.(options, 3, { minVersion: 0, maxVersion: 3 })).toBe(3);
  });
});
