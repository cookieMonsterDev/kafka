import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { DEFAULT_CLIENT_SOFTWARE_NAME, DEFAULT_CLIENT_SOFTWARE_VERSION } from '../v3/request';
import { apiVersionsRequestV4 } from './request';

describe('protocol/requests/api-versions/v4/request', () => {
  it('encodes the same compact software fields as v3 at apiVersion 4', async () => {
    const definition = apiVersionsRequestV4({});
    expect(definition.apiKey).toBe(18);
    expect(definition.apiVersion).toBe(4);
    expect(definition.apiName).toBe('ApiVersions');

    const decoder = new Decoder((await definition.encode()).buffer);
    expect(decoder.readUVarIntString()).toBe(DEFAULT_CLIENT_SOFTWARE_NAME);
    expect(decoder.readUVarIntString()).toBe(DEFAULT_CLIENT_SOFTWARE_VERSION);
    expect(decoder.readUVarInt()).toBe(0);
  });
});
