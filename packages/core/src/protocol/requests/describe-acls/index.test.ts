import { describe, expect, it } from 'vitest';
import { KafkaNonRetriableError } from '../../../errors';
import { RESOURCE_PATTERN_TYPES } from '../../enums/resource-pattern-types';
import { DescribeAcls } from './index';

const literalFilter = {
  resourceType: 2,
  resourceName: 'orders',
  resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
  principal: null,
  host: '*',
  operation: 2,
  permissionType: 3,
};

describe('protocol/requests/describe-acls', () => {
  it('implements versions 0 through 1', () => {
    expect(DescribeAcls.versions).toEqual([0, 1]);
  });

  it('rejects PREFIXED filters on v0', () => {
    expect(() =>
      DescribeAcls.protocol({ version: 0 })({
        ...literalFilter,
        resourcePatternType: RESOURCE_PATTERN_TYPES.PREFIXED,
      }),
    ).toThrow(KafkaNonRetriableError);
  });

  it('ignores LITERAL resourcePatternType on v0', () => {
    expect(() => DescribeAcls.protocol({ version: 0 })(literalFilter)).not.toThrow();
  });
});
