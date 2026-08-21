import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { describeTransactionsRequestV0, requestSchema } from './request';

describe('protocol/requests/describe-transactions/v0/request', () => {
  it('encodes transactional IDs as a compact array', async () => {
    const encoder = await describeTransactionsRequestV0({ transactionalIds: ['tx-a', 'tx-b'] }).encode();

    expect(encoder.buffer).toEqual(Buffer.from([3, 5, 116, 120, 45, 97, 5, 116, 120, 45, 98, 0]));
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual({ transactionalIds: ['tx-a', 'tx-b'] });
  });

  it('encodes an empty transactional ID array', async () => {
    const encoder = await describeTransactionsRequestV0({ transactionalIds: [] }).encode();
    expect(encoder.buffer).toEqual(Buffer.from([1, 0]));
  });
});
