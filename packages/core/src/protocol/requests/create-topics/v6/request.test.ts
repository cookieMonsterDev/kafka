import { describe, expect, it } from 'vitest';
import { withTopicDefaults } from '../v2/request';
import { createTopicsRequestV5 } from '../v5/request';
import { createTopicsRequestV6 } from './request';

const payload = {
  topics: withTopicDefaults([{ topic: 'orders' }]),
  timeout: 5000,
  validateOnly: false,
};

describe('protocol/requests/create-topics/v6/request', () => {
  it('encodes identically to v5, wire-for-wire', async () => {
    const definition = createTopicsRequestV6(payload);
    expect(definition.apiVersion).toBe(6);
    const encoder = await definition.encode();
    const v5 = await createTopicsRequestV5(payload).encode();
    expect(encoder.buffer).toEqual(v5.buffer);
  });
});
