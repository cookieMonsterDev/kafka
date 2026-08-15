import { describe, expect, it } from 'vitest';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { apiVersionsResponseV0 } from './response.js';

function unsupportedVersionResponse(): Buffer {
  return Buffer.from([0, 35, 0, 0, 0, 0]);
}

describe('protocol/requests/api-versions/v0/response', () => {
  it('decodes a real fixture', async () => {
    const data = await apiVersionsResponseV0.decode(Buffer.from(v0ResponseFixture.data));

    expect(data).toEqual({
      errorCode: 0,
      apiVersions: [
        { apiKey: 0, minVersion: 0, maxVersion: 2 },
        { apiKey: 1, minVersion: 0, maxVersion: 3 },
        { apiKey: 2, minVersion: 0, maxVersion: 1 },
        { apiKey: 3, minVersion: 0, maxVersion: 2 },
        { apiKey: 4, minVersion: 0, maxVersion: 0 },
        { apiKey: 5, minVersion: 0, maxVersion: 0 },
        { apiKey: 6, minVersion: 0, maxVersion: 3 },
        { apiKey: 7, minVersion: 1, maxVersion: 1 },
        { apiKey: 8, minVersion: 0, maxVersion: 2 },
        { apiKey: 9, minVersion: 0, maxVersion: 2 },
        { apiKey: 10, minVersion: 0, maxVersion: 0 },
        { apiKey: 11, minVersion: 0, maxVersion: 1 },
        { apiKey: 12, minVersion: 0, maxVersion: 0 },
        { apiKey: 13, minVersion: 0, maxVersion: 0 },
        { apiKey: 14, minVersion: 0, maxVersion: 0 },
        { apiKey: 15, minVersion: 0, maxVersion: 0 },
        { apiKey: 16, minVersion: 0, maxVersion: 0 },
        { apiKey: 17, minVersion: 0, maxVersion: 0 },
        { apiKey: 18, minVersion: 0, maxVersion: 0 },
        { apiKey: 19, minVersion: 0, maxVersion: 1 },
        { apiKey: 20, minVersion: 0, maxVersion: 0 },
      ],
    });

    await expect(apiVersionsResponseV0.parse(data)).resolves.toBeTruthy();
  });

  it('throws a KafkaJSProtocolError if the api is not supported', async () => {
    const data = await apiVersionsResponseV0.decode(unsupportedVersionResponse());
    await expect(apiVersionsResponseV0.parse(data)).rejects.toThrow(/version of API is not supported/);
  });
});
