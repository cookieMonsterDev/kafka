import { describe, expect, it } from 'vitest';
import { API_KEYS } from './api-keys';
import { ApiVersions } from './api-versions/index';
import { KafkaServerDoesNotSupportApiKey } from '../../errors';
import { lookup, NOT_IMPLEMENTED_REQUEST_DEFINITIONS } from './index';

describe('protocol/requests', () => {
  it('picks min(highest version we implement, highest the broker advertised)', () => {
    const factory = lookup({ [API_KEYS.ApiVersions]: { maxVersion: 1 } })(API_KEYS.ApiVersions, ApiVersions);
    expect(factory({}).request.apiVersion).toBe(1);
  });

  it('caps at the highest version we implement even if the broker supports more', () => {
    const factory = lookup({ [API_KEYS.ApiVersions]: { maxVersion: 99 } })(API_KEYS.ApiVersions, ApiVersions);
    expect(factory({}).request.apiVersion).toBe(2);
  });

  it('throws KafkaServerDoesNotSupportApiKey when the broker never advertised the api', () => {
    expect(() => lookup({})(API_KEYS.ApiVersions, ApiVersions)).toThrow(KafkaServerDoesNotSupportApiKey);
  });

  it('the not-implemented marker always throws KafkaNotImplemented', () => {
    expect(() => NOT_IMPLEMENTED_REQUEST_DEFINITIONS.protocol({ version: 0 })).toThrow('This API is not implemented');
  });
});
