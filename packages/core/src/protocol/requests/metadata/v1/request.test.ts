import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder.js';
import { metadataRequestV1 } from './request.js';

describe('protocol/requests/metadata/v1/request', () => {
  it('encodes a non-empty topics array like a plain array', async () => {
    const topics = ['test-topic-1', 'test-topic-2'];
    const definition = metadataRequestV1({ topics });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(new Encoder().writeArray(topics).buffer);
  });

  it('encodes an empty topics array as wire length -1 ("all topics")', async () => {
    const definition = metadataRequestV1({ topics: [] });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(new Encoder().writeInt32(-1).buffer);
  });
});
