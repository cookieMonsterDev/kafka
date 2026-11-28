import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { DEFAULT_CLIENT_SOFTWARE_NAME, DEFAULT_CLIENT_SOFTWARE_VERSION } from '../v3/request';
import { apiVersionsRequestV5 } from './request';

describe('protocol/requests/api-versions/v5/request', () => {
  it('encodes a null cluster id and node id -1 for a bootstrap connection', async () => {
    const definition = apiVersionsRequestV5({});
    expect(definition.apiKey).toBe(18);
    expect(definition.apiVersion).toBe(5);
    expect(definition.apiName).toBe('ApiVersions');

    const decoder = new Decoder((await definition.encode()).buffer);
    expect(decoder.readUVarIntString()).toBe(DEFAULT_CLIENT_SOFTWARE_NAME);
    expect(decoder.readUVarIntString()).toBe(DEFAULT_CLIENT_SOFTWARE_VERSION);
    expect(decoder.readUVarIntString()).toBeNull();
    expect(decoder.readInt32()).toBe(-1);
    expect(decoder.readUVarInt()).toBe(0);
    expect(decoder.canReadBytes(1)).toBe(false);
  });

  it('encodes the expected cluster id and node id after the client software fields', async () => {
    const encoder = await apiVersionsRequestV5({
      clientSoftwareName: 'my-client',
      clientSoftwareVersion: '1.2.3',
      clusterId: 'MkU3OEVBNTcwNTJENDM2Qk',
      nodeId: 2,
    }).encode();

    const decoder = new Decoder(encoder.buffer);
    expect(decoder.readUVarIntString()).toBe('my-client');
    expect(decoder.readUVarIntString()).toBe('1.2.3');
    expect(decoder.readUVarIntString()).toBe('MkU3OEVBNTcwNTJENDM2Qk');
    expect(decoder.readInt32()).toBe(2);
    expect(decoder.readUVarInt()).toBe(0);
    expect(decoder.canReadBytes(1)).toBe(false);
  });
});
