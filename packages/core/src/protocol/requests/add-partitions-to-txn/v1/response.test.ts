import { describe, expect, it } from 'vitest';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { addPartitionsToTxnResponseV1 } from './response';

describe('protocol/requests/add-partitions-to-txn/v1/response', () => {
  it('decodes the v0 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await addPartitionsToTxnResponseV1.decode(Buffer.from(v0ResponseFixture.data));
    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(0);
    expect(data.errors).toHaveLength(1);
    await expect(addPartitionsToTxnResponseV1.parse(data)).resolves.toBeTruthy();
  });
});
