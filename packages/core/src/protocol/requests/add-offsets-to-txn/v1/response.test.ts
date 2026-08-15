import { describe, expect, it } from 'vitest';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { addOffsetsToTxnResponseV1 } from './response.js';

describe('protocol/requests/add-offsets-to-txn/v1/response', () => {
  it('decodes the v0 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await addOffsetsToTxnResponseV1.decode(Buffer.from(v0ResponseFixture.data));
    expect(data).toEqual({ errorCode: 0, throttleTime: 0, clientSideThrottleTime: 0 });
    await expect(addOffsetsToTxnResponseV1.parse(data)).resolves.toBeTruthy();
  });
});
