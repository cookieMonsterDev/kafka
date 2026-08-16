import { describe, expect, it } from 'vitest';
import { KafkaNonRetriableError } from '../../../errors';
import { RESOURCE_PATTERN_TYPES } from '../../enums/resource-pattern-types';
import { DeleteAcls } from './index';

const literalFilter = {
  resourceType: 2,
  resourceName: 'orders',
  resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
  principal: 'User:alice',
  host: '*',
  operation: 2,
  permissionType: 3,
};

describe('protocol/requests/delete-acls', () => {
  it('implements versions 0 through 3', () => {
    expect(DeleteAcls.versions).toEqual([0, 1, 2, 3]);
  });

  it('rejects PREFIXED filters on v0', () => {
    expect(() =>
      DeleteAcls.protocol({ version: 0 })({
        filters: [{ ...literalFilter, resourcePatternType: RESOURCE_PATTERN_TYPES.PREFIXED }],
      }),
    ).toThrow(KafkaNonRetriableError);
  });
});
