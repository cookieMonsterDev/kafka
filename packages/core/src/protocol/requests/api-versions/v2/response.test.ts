import { describe, expect, it } from 'vitest';
import v1ResponseFixture from '../fixtures/v1-response.json' with { type: 'json' };
import v1ResponseMissingThrottleTimeFixture from '../fixtures/v1-response-missing-throttle-time.json' with { type: 'json' };
import { apiVersionsResponseV2 } from './response.js';

function unsupportedVersionResponse(): Buffer {
  return Buffer.from([0, 35, 0, 0, 0, 0]);
}

describe('protocol/requests/api-versions/v2/response', () => {
  it('decodes the v1 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await apiVersionsResponseV2.decode(Buffer.from(v1ResponseFixture.data));

    expect(data.errorCode).toBe(0);
    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(0);
    expect(data.apiVersions).toHaveLength(34);
    await expect(apiVersionsResponseV2.parse(data)).resolves.toBeTruthy();
  });

  it('defaults throttle_time_ms to 0 when the broker omits it (kafkajs#491)', async () => {
    const data = await apiVersionsResponseV2.decode(Buffer.from(v1ResponseMissingThrottleTimeFixture.data));
    expect(data.clientSideThrottleTime).toBe(0);
  });

  it('throws a KafkaJSProtocolError if the api is not supported', async () => {
    const data = await apiVersionsResponseV2.decode(unsupportedVersionResponse());
    await expect(apiVersionsResponseV2.parse(data)).rejects.toThrow(/version of API is not supported/);
  });
});
