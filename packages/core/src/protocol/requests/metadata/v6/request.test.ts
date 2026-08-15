import { describe, expect, it } from 'vitest';
import { metadataRequestV6 } from './request.js';

describe('protocol/requests/metadata/v6/request', () => {
  it('carries apiVersion 6', () => {
    expect(metadataRequestV6({ topics: [], allowAutoTopicCreation: true }).apiVersion).toBe(6);
  });
});
