import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder.js';
import { metadataRequestV4 } from './request.js';

describe('protocol/requests/metadata/v4/request', () => {
  it('encodes topics then allowAutoTopicCreation', async () => {
    const topics = ['test-topic-1', 'test-topic-2'];
    const definition = metadataRequestV4({ topics, allowAutoTopicCreation: true });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(new Encoder().writeArray(topics).writeBoolean(true).buffer);
  });
});
