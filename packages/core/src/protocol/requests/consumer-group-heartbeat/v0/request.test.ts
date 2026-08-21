import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { consumerGroupHeartbeatRequestV0, requestSchema } from './request';

const topicId = Buffer.from('0123456789abcdef');

describe('protocol/requests/consumer-group-heartbeat/v0/request', () => {
  it('encodes a join with subscribed topics and null owned partitions', async () => {
    const payload = {
      groupId: 'g',
      memberId: 'm',
      memberEpoch: 0,
      instanceId: null as string | null,
      rackId: null as string | null,
      rebalanceTimeoutMs: 60_000,
      subscribedTopicNames: ['events'] as string[] | null,
      serverAssignor: null as string | null,
      topicPartitions: null as { topicId: Buffer; partitions: number[] }[] | null,
    };
    const encoder = await consumerGroupHeartbeatRequestV0(payload).encode();
    const expected = new Encoder()
      .writeUVarIntString('g')
      .writeUVarIntString('m')
      .writeInt32(0)
      .writeUVarIntString(null)
      .writeUVarIntString(null)
      .writeInt32(60_000)
      .writeUVarInt(2)
      .writeUVarIntString('events')
      .writeUVarIntString(null)
      .writeUVarInt(0)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('encodes owned topic partitions when they changed', async () => {
    const payload = {
      groupId: 'g',
      memberId: 'm',
      memberEpoch: 1,
      instanceId: 'instance-1' as string | null,
      rackId: 'rack-a' as string | null,
      rebalanceTimeoutMs: -1,
      subscribedTopicNames: null as string[] | null,
      serverAssignor: 'uniform' as string | null,
      topicPartitions: [{ topicId, partitions: [0, 1] }],
    };
    const encoder = await consumerGroupHeartbeatRequestV0(payload).encode();
    const expected = new Encoder()
      .writeUVarIntString('g')
      .writeUVarIntString('m')
      .writeInt32(1)
      .writeUVarIntString('instance-1')
      .writeUVarIntString('rack-a')
      .writeInt32(-1)
      .writeUVarInt(0)
      .writeUVarIntString('uniform')
      .writeUVarInt(2)
      .writeBuffer(topicId)
      .writeUVarInt(3)
      .writeInt32(0)
      .writeInt32(1)
      .writeUVarInt(0)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });
});
