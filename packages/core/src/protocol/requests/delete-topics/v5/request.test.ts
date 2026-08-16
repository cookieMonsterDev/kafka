import { describe, expect, it } from 'vitest';
import { deleteTopicsRequestV4 } from '../v4/request';
import { deleteTopicsRequestV5 } from './request';

const payload = {
  topics: ['orders', 'payments'],
  timeout: 5000,
};

describe('protocol/requests/delete-topics/v5/request', () => {
  it('encodes identically to v4, wire-for-wire', async () => {
    const definition = deleteTopicsRequestV5(payload);
    expect(definition.apiVersion).toBe(5);
    const encoder = await definition.encode();
    const v4 = await deleteTopicsRequestV4(payload).encode();
    expect(encoder.buffer).toEqual(v4.buffer);
  });
});
