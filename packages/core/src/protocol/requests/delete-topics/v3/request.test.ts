import { describe, expect, it } from 'vitest';
import { deleteTopicsRequestV1 } from '../v1/request';
import { deleteTopicsRequestV3 } from './request';

const payload = {
  topics: ['orders', 'payments'],
  timeout: 5000,
};

describe('protocol/requests/delete-topics/v3/request', () => {
  it('encodes identically to v1, wire-for-wire', async () => {
    const definition = deleteTopicsRequestV3(payload);
    expect(definition.apiVersion).toBe(3);
    const encoder = await definition.encode();
    const v1 = await deleteTopicsRequestV1(payload).encode();
    expect(encoder.buffer).toEqual(v1.buffer);
  });
});
