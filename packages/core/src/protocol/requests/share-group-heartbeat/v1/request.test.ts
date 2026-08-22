import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { shareGroupHeartbeatRequestV1, requestSchema } from './request';

describe('protocol/requests/share-group-heartbeat/v1/request', () => {
  it('encodes a join with subscribed topics', async () => {
    const payload = {
      groupId: 'g',
      memberId: 'm',
      memberEpoch: 0,
      rackId: null as string | null,
      subscribedTopicNames: ['events'] as string[] | null,
    };
    const encoder = await shareGroupHeartbeatRequestV1(payload).encode();
    const expected = new Encoder()
      .writeUVarIntString('g')
      .writeUVarIntString('m')
      .writeInt32(0)
      .writeUVarIntString(null)
      .writeUVarInt(2)
      .writeUVarIntString('events')
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('encodes null subscribed topics distinctly from an empty list', async () => {
    const payload = {
      groupId: 'g',
      memberId: 'm',
      memberEpoch: 1,
      rackId: 'rack-a' as string | null,
      subscribedTopicNames: null as string[] | null,
    };
    const encoder = await shareGroupHeartbeatRequestV1(payload).encode();
    const expected = new Encoder()
      .writeUVarIntString('g')
      .writeUVarIntString('m')
      .writeInt32(1)
      .writeUVarIntString('rack-a')
      .writeUVarInt(0)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });
});
