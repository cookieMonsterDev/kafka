import { describe, expect, it } from 'vitest';
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { describeGroupsRequestV0 } from './request';

describe('protocol/requests/describe-groups/v0/request', () => {
  it('encodes to match the real fixture', async () => {
    const definition = describeGroupsRequestV0({ groupIds: ['consumer-group-id-608e7e42043d917ecb44'] });
    const encoder = await definition.encode();
    expect(encoder.buffer).toEqual(Buffer.from(v0RequestFixture.data));
  });
});
