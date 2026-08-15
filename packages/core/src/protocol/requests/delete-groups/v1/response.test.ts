import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder.js';
import { deleteGroupsResponseV1 } from './response.js';

describe('protocol/requests/delete-groups/v1/response', () => {
  it('decodes the v0 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const wire = new Encoder().writeInt32(7).writeInt32(0).buffer;
    const data = await deleteGroupsResponseV1.decode(wire);
    expect(data).toEqual({ throttleTime: 0, clientSideThrottleTime: 7, results: [] });
  });
});
