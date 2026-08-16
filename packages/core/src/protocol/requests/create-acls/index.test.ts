import { describe, expect, it } from 'vitest';
import { KafkaNonRetriableError } from '../../../errors';
import { RESOURCE_PATTERN_TYPES } from '../../enums/resource-pattern-types';
import { CreateAcls } from './index';

const literalCreation = {
  resourceType: 2,
  resourceName: 'orders',
  resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
  principal: 'User:alice',
  host: '*',
  operation: 2,
  permissionType: 3,
};

describe('protocol/requests/create-acls', () => {
  it('implements versions 0 through 1', () => {
    expect(CreateAcls.versions).toEqual([0, 1]);
  });

  it('rejects PREFIXED creations on v0', () => {
    expect(() =>
      CreateAcls.protocol({ version: 0 })({
        creations: [{ ...literalCreation, resourcePatternType: RESOURCE_PATTERN_TYPES.PREFIXED }],
      }),
    ).toThrow(KafkaNonRetriableError);
  });
});
