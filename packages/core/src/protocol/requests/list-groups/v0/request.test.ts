import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder.js';
import { listGroupsRequestV0 } from './request.js';

describe('protocol/requests/list-groups/v0/request', () => {
  it('encodes to an empty body', async () => {
    const definition = listGroupsRequestV0({});
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(new Encoder().buffer);
  });
});
