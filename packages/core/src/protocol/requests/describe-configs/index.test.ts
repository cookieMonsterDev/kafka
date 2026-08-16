import { describe, expect, it } from 'vitest';
import { CONFIG_RESOURCE_TYPES } from '../../enums/config-resource-types';
import { DescribeConfigs } from './index';

describe('protocol/requests/describe-configs', () => {
  it('implements versions 0 through 4', () => {
    expect(DescribeConfigs.versions).toEqual([0, 1, 2, 3, 4]);
  });

  it('ignores includeSynonyms on v0', () => {
    expect(() =>
      DescribeConfigs.protocol({ version: 0 })({
        resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: 't', configNames: [] }],
        includeSynonyms: true,
      }),
    ).not.toThrow();
  });

  it('ignores includeDocumentation on v0–v2', () => {
    for (const version of [0, 1, 2]) {
      expect(() =>
        DescribeConfigs.protocol({ version })({
          resources: [{ type: CONFIG_RESOURCE_TYPES.TOPIC, name: 't', configNames: [] }],
          includeDocumentation: true,
        }),
      ).not.toThrow();
    }
  });
});
