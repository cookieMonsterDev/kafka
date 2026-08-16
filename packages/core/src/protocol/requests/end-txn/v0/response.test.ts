import { describe, expect, it } from 'vitest';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { endTxnResponseV0 } from './response';

describe('protocol/requests/end-txn/v0/response', () => {
  it('decodes a real fixture', async () => {
    const data = await endTxnResponseV0.decode(Buffer.from(v0ResponseFixture.data));
    expect(data).toEqual({ throttleTime: 0, errorCode: 0 });
    await expect(endTxnResponseV0.parse(data)).resolves.toBeTruthy();
  });

  it('throws if the version is not supported', async () => {
    const data = await endTxnResponseV0.decode(Buffer.from([0, 0, 0, 0, 0, 35]));
    await expect(endTxnResponseV0.parse(data)).rejects.toThrow(/The version of API is not supported/);
  });
});
