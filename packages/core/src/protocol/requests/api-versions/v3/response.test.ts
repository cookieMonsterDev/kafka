import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { apiVersionsResponseV3 } from './response';

function v3ResponseBuffer(errorCode: number, apiKeys: { apiKey: number; minVersion: number; maxVersion: number }[]) {
  const entries = apiKeys.map((entry) =>
    new Encoder().writeInt16(entry.apiKey).writeInt16(entry.minVersion).writeInt16(entry.maxVersion).writeUVarInt(0),
  );

  return new Encoder().writeInt16(errorCode).writeUVarIntArray(entries).writeInt32(0).writeUVarInt(0).buffer;
}

describe('protocol/requests/api-versions/v3/response', () => {
  it('decodes a compact api-keys array and remaps throttleTime', async () => {
    const data = await apiVersionsResponseV3.decode(
      v3ResponseBuffer(0, [
        { apiKey: 0, minVersion: 3, maxVersion: 10 },
        { apiKey: 18, minVersion: 0, maxVersion: 4 },
      ]),
    );

    expect(data).toEqual({
      errorCode: 0,
      apiVersions: [
        { apiKey: 0, minVersion: 3, maxVersion: 10 },
        { apiKey: 18, minVersion: 0, maxVersion: 4 },
      ],
      throttleTime: 0,
      clientSideThrottleTime: 0,
    });
    await expect(apiVersionsResponseV3.parse(data)).resolves.toBeTruthy();
  });

  it('throws a KafkaProtocolError if the api is not supported', async () => {
    const data = await apiVersionsResponseV3.decode(v3ResponseBuffer(35, []));
    await expect(apiVersionsResponseV3.parse(data)).rejects.toThrow(/version of API is not supported/);
  });
});
