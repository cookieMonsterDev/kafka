import { describe, expect, it } from 'vitest';
import { metadataRequestV12 } from '../v12/request';
import { metadataRequestV13 } from './request';

describe('protocol/requests/metadata/v13/request', () => {
  it('matches the v12 body and reports apiVersion 13', async () => {
    const payload = { topics: ['orders'], allowAutoTopicCreation: true };
    const definition = metadataRequestV13(payload);
    expect(definition.apiVersion).toBe(13);

    const encoder = await definition.encode();
    const v12 = await metadataRequestV12(payload).encode();
    expect(encoder.buffer).toEqual(v12.buffer);
  });
});
