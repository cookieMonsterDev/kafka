import { describe, expect, it } from 'vitest';
import { CONFIG_RESOURCE_TYPES } from '../../enums/config-resource-types';
import { DescribeConfigs } from './index';

describe('protocol/requests/describe-configs', () => {
  it('implements versions 0 through 2', () => {
    expect(DescribeConfigs.versions).toEqual([0, 1, 2]);
  });

  it('ignores includeSynonyms on v0', () => {
    expect(() =>
      DescribeConfigs.protocol({ version: 0 })({
        resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: 't', configNames: [] }],
        includeSynonyms: true,
      }),
    ).not.toThrow();
  });
});
