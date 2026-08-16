import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { apiVersionsRequestV3, DEFAULT_CLIENT_SOFTWARE_NAME, DEFAULT_CLIENT_SOFTWARE_VERSION } from './request';

describe('protocol/requests/api-versions/v3/request', () => {
  it('encodes compact client software fields rather than an empty body', async () => {
    const definition = apiVersionsRequestV3({});
    const encoder = await definition.encode();

    expect(definition.apiKey).toBe(18);
    expect(definition.apiVersion).toBe(3);
    expect(definition.apiName).toBe('ApiVersions');
    expect(encoder.buffer.length).toBeGreaterThan(0);

    const decoder = new Decoder(encoder.buffer);
    expect(decoder.readUVarIntString()).toBe(DEFAULT_CLIENT_SOFTWARE_NAME);
    expect(decoder.readUVarIntString()).toBe(DEFAULT_CLIENT_SOFTWARE_VERSION);
    expect(decoder.readUVarInt()).toBe(0);
  });

  it('starts with a compact-string uvarint length, not a length-prefixed STRING', async () => {
    const encoder = await apiVersionsRequestV3({}).encode();
    // "kafka" is 5 bytes; compact string length is N+1 = 6 as a single-byte uvarint.
    expect(encoder.buffer[0]).toBe(6);
  });

  it('encodes custom client software name and version', async () => {
    const encoder = await apiVersionsRequestV3({
      clientSoftwareName: 'my-client',
      clientSoftwareVersion: '1.2.3',
    }).encode();

    const decoder = new Decoder(encoder.buffer);
    expect(decoder.readUVarIntString()).toBe('my-client');
    expect(decoder.readUVarIntString()).toBe('1.2.3');
    expect(decoder.readUVarInt()).toBe(0);
  });
});
