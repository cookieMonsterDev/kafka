import { describe, expect, it } from 'vitest';
import v1ResponseFixture from '../fixtures/v1-response.json' with { type: 'json' };
import v1ResponseMissingThrottleTimeFixture from '../fixtures/v1-response-missing-throttle-time.json' with { type: 'json' };
import { apiVersionsResponseV1 } from './response.js';

function unsupportedVersionResponse(): Buffer {
  return Buffer.from([0, 35, 0, 0, 0, 0]);
}

const EXPECTED_API_VERSIONS = [
  { apiKey: 0, maxVersion: 3, minVersion: 0 },
  { apiKey: 1, maxVersion: 5, minVersion: 0 },
  { apiKey: 2, maxVersion: 2, minVersion: 0 },
  { apiKey: 3, maxVersion: 4, minVersion: 0 },
  { apiKey: 4, maxVersion: 0, minVersion: 0 },
  { apiKey: 5, maxVersion: 0, minVersion: 0 },
  { apiKey: 6, maxVersion: 3, minVersion: 0 },
  { apiKey: 7, maxVersion: 1, minVersion: 1 },
  { apiKey: 8, maxVersion: 3, minVersion: 0 },
  { apiKey: 9, maxVersion: 3, minVersion: 0 },
  { apiKey: 10, maxVersion: 1, minVersion: 0 },
  { apiKey: 11, maxVersion: 2, minVersion: 0 },
  { apiKey: 12, maxVersion: 1, minVersion: 0 },
  { apiKey: 13, maxVersion: 1, minVersion: 0 },
  { apiKey: 14, maxVersion: 1, minVersion: 0 },
  { apiKey: 15, maxVersion: 1, minVersion: 0 },
  { apiKey: 16, maxVersion: 1, minVersion: 0 },
  { apiKey: 17, maxVersion: 0, minVersion: 0 },
  { apiKey: 18, maxVersion: 1, minVersion: 0 },
  { apiKey: 19, maxVersion: 2, minVersion: 0 },
  { apiKey: 20, maxVersion: 1, minVersion: 0 },
  { apiKey: 21, maxVersion: 0, minVersion: 0 },
  { apiKey: 22, maxVersion: 0, minVersion: 0 },
  { apiKey: 23, maxVersion: 0, minVersion: 0 },
  { apiKey: 24, maxVersion: 0, minVersion: 0 },
  { apiKey: 25, maxVersion: 0, minVersion: 0 },
  { apiKey: 26, maxVersion: 0, minVersion: 0 },
  { apiKey: 27, maxVersion: 0, minVersion: 0 },
  { apiKey: 28, maxVersion: 0, minVersion: 0 },
  { apiKey: 29, maxVersion: 0, minVersion: 0 },
  { apiKey: 30, maxVersion: 0, minVersion: 0 },
  { apiKey: 31, maxVersion: 0, minVersion: 0 },
  { apiKey: 32, maxVersion: 0, minVersion: 0 },
  { apiKey: 33, maxVersion: 0, minVersion: 0 },
];

describe('protocol/requests/api-versions/v1/response', () => {
  it('decodes a real fixture', async () => {
    const data = await apiVersionsResponseV1.decode(Buffer.from(v1ResponseFixture.data));

    expect(data).toEqual({ apiVersions: EXPECTED_API_VERSIONS, errorCode: 0, throttleTime: 0 });
    await expect(apiVersionsResponseV1.parse(data)).resolves.toBeTruthy();
  });

  it('defaults throttle_time_ms to 0 when the broker omits it (kafkajs#491)', async () => {
    const data = await apiVersionsResponseV1.decode(Buffer.from(v1ResponseMissingThrottleTimeFixture.data));
    expect(data).toEqual({ apiVersions: EXPECTED_API_VERSIONS, errorCode: 0, throttleTime: 0 });
  });

  it('throws a KafkaJSProtocolError if the api is not supported', async () => {
    const data = await apiVersionsResponseV1.decode(unsupportedVersionResponse());
    await expect(apiVersionsResponseV1.parse(data)).rejects.toThrow(/version of API is not supported/);
  });
});
