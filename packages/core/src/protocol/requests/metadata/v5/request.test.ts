import { describe, expect, it } from 'vitest';
import { metadataRequestV5 } from './request';

describe('protocol/requests/metadata/v5/request', () => {
  it('carries apiVersion 5', () => {
    expect(metadataRequestV5({ topics: [], allowAutoTopicCreation: true }).apiVersion).toBe(5);
  });
});
