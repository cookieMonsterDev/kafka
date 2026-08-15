import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder.js';
import { metadataRequestV2 } from './request.js';

describe('protocol/requests/metadata/v2/request', () => {
  it('encodes like v1 (nullableArray topics)', async () => {
    const definition = metadataRequestV2({ topics: ['a'] });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(new Encoder().writeArray(['a']).buffer);
    expect(definition.apiVersion).toBe(2);
  });
});
