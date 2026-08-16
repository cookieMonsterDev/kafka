import { describe, expect, it } from 'vitest';
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { apiVersionsRequestV0 } from './request';

describe('protocol/requests/api-versions/v0/request', () => {
  it('encodes to an empty body, matching the real fixture', async () => {
    const definition = apiVersionsRequestV0({});
    const encoder = await definition.encode();

    expect(definition.apiKey).toBe(18);
    expect(definition.apiVersion).toBe(0);
    expect(definition.apiName).toBe('ApiVersions');
    expect(encoder.buffer).toEqual(Buffer.from(v0RequestFixture.data));
  });
});
