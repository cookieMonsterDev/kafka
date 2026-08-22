import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { listConfigResourcesRequestV0, requestSchema } from './request';

describe('protocol/requests/list-config-resources/v0/request', () => {
  it('encodes an empty flexible body', async () => {
    const encoder = await listConfigResourcesRequestV0({}).encode();

    expect(encoder.buffer).toEqual(Buffer.from([0]));
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual({});
  });
});
