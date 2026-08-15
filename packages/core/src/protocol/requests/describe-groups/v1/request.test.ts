import { describe, expect, it } from 'vitest';
import v1RequestFixture from '../fixtures/v1-request.json' with { type: 'json' };
import { describeGroupsRequestV1 } from './request.js';

describe('protocol/requests/describe-groups/v1/request', () => {
  it('encodes to match the real fixture', async () => {
    const definition = describeGroupsRequestV1({
      groupIds: ['consumer-group-id-4de0aa10ef94403a397d-53384-d2fee969-1446-4166-bc8e-c88e8daffdfe'],
    });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v1RequestFixture.data));
  });
});
