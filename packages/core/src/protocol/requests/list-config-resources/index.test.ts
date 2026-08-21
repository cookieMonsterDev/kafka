import { describe, expect, it } from 'vitest';
import { KafkaNonRetriableError } from '../../../errors';
import { CONFIG_RESOURCE_TYPES } from '../../enums/config-resource-types';
import { ListConfigResources } from './index';

describe('protocol/requests/list-config-resources', () => {
  it('implements versions 0 and 1', () => {
    expect(ListConfigResources.versions).toEqual([0, 1]);
  });

  it('creates a version 0 request', () => {
    const { request } = ListConfigResources.protocol({ version: 0 })({});
    expect(request).toMatchObject({ apiKey: 74, apiVersion: 0, apiName: 'ListConfigResources' });
  });

  it('creates a version 1 request', () => {
    const { request } = ListConfigResources.protocol({ version: 1 })({
      resourceTypes: [CONFIG_RESOURCE_TYPES.TOPIC],
    });
    expect(request).toMatchObject({ apiKey: 74, apiVersion: 1, apiName: 'ListConfigResources' });
  });

  it('rejects resourceTypes on v0 instead of silently listing client metrics', () => {
    expect(() =>
      ListConfigResources.protocol({ version: 0 })({ resourceTypes: [CONFIG_RESOURCE_TYPES.TOPIC] }),
    ).toThrow(KafkaNonRetriableError);
  });

  it('allows an empty resourceTypes list on v0', () => {
    const { request } = ListConfigResources.protocol({ version: 0 })({ resourceTypes: [] });
    expect(request.apiVersion).toBe(0);
  });
});
