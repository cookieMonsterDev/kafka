import { describe, expect, it } from 'vitest';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { addOffsetsToTxnResponseV0 } from './response.js';

describe('protocol/requests/add-offsets-to-txn/v0/response', () => {
  it('decodes a real fixture', async () => {
    const data = await addOffsetsToTxnResponseV0.decode(Buffer.from(v0ResponseFixture.data));
    expect(data).toEqual({ throttleTime: 0, errorCode: 0 });
    await expect(addOffsetsToTxnResponseV0.parse(data)).resolves.toBeTruthy();
  });

  it('throws if the version is not supported', async () => {
    const data = await addOffsetsToTxnResponseV0.decode(Buffer.from([0, 0, 0, 0, 0, 35]));
    await expect(addOffsetsToTxnResponseV0.parse(data)).rejects.toThrow(/The version of API is not supported/);
  });
});
