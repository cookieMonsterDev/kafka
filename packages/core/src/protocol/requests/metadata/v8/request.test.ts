import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { metadataRequestV6 } from '../v6/request';
import { metadataRequestV8 } from './request';

describe('protocol/requests/metadata/v8/request', () => {
  it('appends the two authorized-operations include flags after the v6 body', async () => {
    const payload = { topics: ['orders'], allowAutoTopicCreation: true };
    const definition = metadataRequestV8(payload);
    expect(definition.apiVersion).toBe(8);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeNullableArray(['orders'], 'string')
      .writeBoolean(true)
      .writeBoolean(false)
      .writeBoolean(false);

    expect(encoder.buffer).toEqual(expected.buffer);

    const v6 = await metadataRequestV6(payload).encode();
    expect(encoder.buffer.subarray(0, v6.buffer.length)).toEqual(v6.buffer);
  });
});
