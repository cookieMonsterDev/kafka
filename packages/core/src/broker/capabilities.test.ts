import { describe, expect, it } from 'vitest';
import { API_KEYS } from '../protocol/requests/api-keys';
import type { BrokerVersions } from '../protocol/requests/index';
import {
  supportsAclPatternType,
  supportsHeaders,
  supportsRecordBatch,
  supportsTransactions,
  supportsTransactionV2,
  supportsZstd,
} from './capabilities';

describe('broker/capabilities', () => {
  it('supportsRecordBatch when Produce maxVersion is at least 3', () => {
    expect(supportsRecordBatch({ [API_KEYS.Produce]: { maxVersion: 2 } })).toBe(false);
    expect(supportsRecordBatch({ [API_KEYS.Produce]: { maxVersion: 3 } })).toBe(true);
    expect(supportsRecordBatch({})).toBe(false);
  });

  it('treats headers as the same capability as RecordBatch', () => {
    const versions: BrokerVersions = { [API_KEYS.Produce]: { maxVersion: 3 } };
    expect(supportsHeaders(versions)).toBe(supportsRecordBatch(versions));
  });

  it('supportsTransactions when InitProducerId is advertised', () => {
    expect(supportsTransactions({})).toBe(false);
    expect(supportsTransactions({ [API_KEYS.InitProducerId]: { minVersion: 0, maxVersion: 0 } })).toBe(true);
  });

  it('supportsZstd when Produce maxVersion is at least 7', () => {
    expect(supportsZstd({ [API_KEYS.Produce]: { maxVersion: 6 } })).toBe(false);
    expect(supportsZstd({ [API_KEYS.Produce]: { maxVersion: 7 } })).toBe(true);
  });

  it('supportsAclPatternType when DescribeAcls maxVersion is at least 1', () => {
    expect(supportsAclPatternType({ [API_KEYS.DescribeAcls]: { maxVersion: 0 } })).toBe(false);
    expect(supportsAclPatternType({ [API_KEYS.DescribeAcls]: { maxVersion: 1 } })).toBe(true);
    expect(supportsAclPatternType({})).toBe(false);
  });

  it('supportsTransactionV2 when Produce maxVersion is at least 12', () => {
    expect(supportsTransactionV2({ [API_KEYS.Produce]: { maxVersion: 11 } })).toBe(false);
    expect(supportsTransactionV2({ [API_KEYS.Produce]: { maxVersion: 12 } })).toBe(true);
    expect(supportsTransactionV2({})).toBe(false);
  });
});
