import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { joinGroupRequestV8, requestSchema } from '../v8/request';
import { withDefaultMetadata } from '../v0/request';
import { joinGroupRequestV9 } from './request';

const payload = {
  groupId: 'g',
  sessionTimeout: 30_000,
  rebalanceTimeout: 60_000,
  memberId: '',
  groupInstanceId: 'instance-1',
  protocolType: 'consumer',
  groupProtocols: withDefaultMetadata([{ name: 'AssignerName', metadata: Buffer.from('meta') }]),
  reason: null as string | null,
};

describe('protocol/requests/join-group/v9/request', () => {
  it('uses api version 9 with the same compact body as v8', async () => {
    const definition = joinGroupRequestV9(payload);
    expect(definition.apiVersion).toBe(9);

    const encoder = await definition.encode();
    const v8 = await joinGroupRequestV8(payload).encode();
    expect(encoder.buffer).toEqual(v8.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });
});
