import { describe, expect, it } from 'vitest';
import { findCoordinatorRequestV4 } from '../v4/request';
import { findCoordinatorRequestV5 } from './request';

const payload = { coordinatorType: 1, coordinatorKeys: ['tx-a', 'tx-b'] };

describe('protocol/requests/find-coordinator/v5/request', () => {
  it('encodes the same body as v4 with apiVersion 5', async () => {
    const definition = findCoordinatorRequestV5(payload);
    expect(definition.apiVersion).toBe(5);
    const v5 = await definition.encode();
    const v4 = await findCoordinatorRequestV4(payload).encode();
    expect(v5.buffer).toEqual(v4.buffer);
  });
});
