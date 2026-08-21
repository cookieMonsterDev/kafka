import { describe, expect, it } from 'vitest';
import { Decoder } from '../../decoder';
import { ListTransactions } from './index';
import { requestSchema as requestSchemaV0 } from './v0/request';
import { requestSchema as requestSchemaV1 } from './v1/request';
import { requestSchema as requestSchemaV2 } from './v2/request';

describe('protocol/requests/list-transactions', () => {
  it('implements versions 0, 1, and 2', () => {
    expect(ListTransactions.versions).toEqual([0, 1, 2]);
  });

  it('creates a version 0 request without v1/v2 fields', async () => {
    const { request } = ListTransactions.protocol({ version: 0 })({
      stateFilters: ['Ongoing'],
      producerIdFilters: [1n],
      durationFilter: 5_000n,
      transactionalIdPattern: 'tx-*',
    });
    expect(request).toMatchObject({ apiKey: 66, apiVersion: 0, apiName: 'ListTransactions' });

    const encoder = await request.encode();
    expect(requestSchemaV0.read(new Decoder(encoder.buffer))).toEqual({
      stateFilters: ['Ongoing'],
      producerIdFilters: [1n],
    });
  });

  it('creates a version 1 request without the v2 pattern field', async () => {
    const { request } = ListTransactions.protocol({ version: 1 })({
      durationFilter: 2_000n,
      transactionalIdPattern: 'tx-*',
    });
    expect(request).toMatchObject({ apiKey: 66, apiVersion: 1, apiName: 'ListTransactions' });

    const encoder = await request.encode();
    expect(requestSchemaV1.read(new Decoder(encoder.buffer))).toEqual({
      stateFilters: [],
      producerIdFilters: [],
      durationFilter: 2_000n,
    });
  });

  it('creates a version 2 request with the pattern field', async () => {
    const { request } = ListTransactions.protocol({ version: 2 })({
      transactionalIdPattern: 'payments-*',
    });
    expect(request).toMatchObject({ apiKey: 66, apiVersion: 2, apiName: 'ListTransactions' });

    const encoder = await request.encode();
    expect(requestSchemaV2.read(new Decoder(encoder.buffer))).toEqual({
      stateFilters: [],
      producerIdFilters: [],
      durationFilter: -1n,
      transactionalIdPattern: 'payments-*',
    });
  });
});
