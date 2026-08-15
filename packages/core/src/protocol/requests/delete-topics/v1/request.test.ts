import { describe, expect, it } from 'vitest';
import v1RequestFixture from '../fixtures/v1-request.json' with { type: 'json' };
import { deleteTopicsRequestV1 } from './request.js';

describe('protocol/requests/delete-topics/v1/request', () => {
  it('encodes a real fixture', async () => {
    const encoder = await deleteTopicsRequestV1({
      topics: [
        'test-topic-386ea404396d663a8042-56298-e6e26331-de25-48d8-90b6-4710cd0b618b',
        'test-topic-bb5d4c0c37ae53eb8b53-56298-ac202bf8-78e7-4d8b-ad07-4e01d8148db0',
      ],
      timeout: 5000,
    }).encode();

    expect(encoder.buffer).toEqual(Buffer.from(v1RequestFixture.data));
  });
});
