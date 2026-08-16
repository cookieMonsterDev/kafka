import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { joinGroupRequestV6, requestSchema } from '../v6/request';
import { withDefaultMetadata } from '../v0/request';
import { joinGroupRequestV7 } from './request';

const payload = {
  groupId: 'g',
  sessionTimeout: 30_000,
  rebalanceTimeout: 60_000,
  memberId: '',
  groupInstanceId: 'instance-1',
  protocolType: 'consumer',
  groupProtocols: withDefaultMetadata([{ name: 'AssignerName', metadata: Buffer.from('meta') }]),
};

describe('protocol/requests/join-group/v7/request', () => {
  it('uses api version 7 with the same compact body as v6', async () => {
    const definition = joinGroupRequestV7(payload);
    expect(definition.apiVersion).toBe(7);

    const encoder = await definition.encode();
    const v6 = await joinGroupRequestV6(payload).encode();
    expect(encoder.buffer).toEqual(v6.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });
});
