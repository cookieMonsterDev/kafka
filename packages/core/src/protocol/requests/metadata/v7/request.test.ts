import { describe, expect, it } from 'vitest';
import { metadataRequestV6 } from '../v6/request';
import { metadataRequestV7 } from './request';

describe('protocol/requests/metadata/v7/request', () => {
  it('carries apiVersion 7 with the same body as v6', async () => {
    const payload = { topics: ['orders'], allowAutoTopicCreation: true };
    const definition = metadataRequestV7(payload);
    expect(definition.apiVersion).toBe(7);

    const v7 = await definition.encode();
    const v6 = await metadataRequestV6(payload).encode();
    expect(v7.buffer).toEqual(v6.buffer);
  });
});
