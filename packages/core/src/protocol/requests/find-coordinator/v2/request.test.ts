import { describe, expect, it } from 'vitest';
import v1RequestFixture from '../fixtures/v1-request.json' with { type: 'json' };
import { findCoordinatorRequestV2 } from './request.js';

describe('protocol/requests/find-coordinator/v2/request', () => {
  it('encodes to match the real fixture (identical wire format to v1)', async () => {
    const definition = findCoordinatorRequestV2({ coordinatorKey: 'group-id', coordinatorType: 0 });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v1RequestFixture.data));
  });
});
