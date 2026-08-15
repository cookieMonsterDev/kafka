import { describe, expect, it } from 'vitest';
import v1RequestFixture from '../fixtures/v1-request.json' with { type: 'json' };
import { findCoordinatorRequestV1 } from './request.js';

describe('protocol/requests/find-coordinator/v1/request', () => {
  it('encodes to match the real fixture', async () => {
    const definition = findCoordinatorRequestV1({ coordinatorKey: 'group-id', coordinatorType: 0 });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v1RequestFixture.data));
  });
});
