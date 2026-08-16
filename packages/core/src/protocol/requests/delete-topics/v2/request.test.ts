import { describe, expect, it } from 'vitest';
import v1RequestFixture from '../fixtures/v1-request.json' with { type: 'json' };
import { deleteTopicsRequestV1 } from '../v1/request';
import { deleteTopicsRequestV2 } from './request';

const payload = {
  topics: [
    'test-topic-386ea404396d663a8042-56298-e6e26331-de25-48d8-90b6-4710cd0b618b',
    'test-topic-bb5d4c0c37ae53eb8b53-56298-ac202bf8-78e7-4d8b-ad07-4e01d8148db0',
  ],
  timeout: 5000,
};

describe('protocol/requests/delete-topics/v2/request', () => {
  it('encodes identically to v1, wire-for-wire', async () => {
    const definition = deleteTopicsRequestV2(payload);
    expect(definition.apiVersion).toBe(2);
    const encoder = await definition.encode();
    const v1 = await deleteTopicsRequestV1(payload).encode();
    expect(encoder.buffer).toEqual(v1.buffer);
    expect(encoder.buffer).toEqual(Buffer.from(v1RequestFixture.data));
  });
});
