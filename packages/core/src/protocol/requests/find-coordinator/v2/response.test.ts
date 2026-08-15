import { describe, expect, it } from 'vitest';
import v1ResponseFixture from '../fixtures/v1-response.json' with { type: 'json' };
import v1ResponseVersionErrorFixture from '../fixtures/v1-response-version-error.json' with { type: 'json' };
import { findCoordinatorResponseV2 } from './response.js';

describe('protocol/requests/find-coordinator/v2/response', () => {
  it('decodes the v1 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await findCoordinatorResponseV2.decode(Buffer.from(v1ResponseFixture.data));
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      errorMessage: null,
      coordinator: { nodeId: 2, host: '192.168.50.211', port: 9098 },
    });
    await expect(findCoordinatorResponseV2.parse(data)).resolves.toBeTruthy();
  });

  it('throws a KafkaJSProtocolError if the api is not supported', async () => {
    const data = await findCoordinatorResponseV2.decode(Buffer.from(v1ResponseVersionErrorFixture.data));
    await expect(findCoordinatorResponseV2.parse(data)).rejects.toThrow(/version of API is not supported/);
  });
});
