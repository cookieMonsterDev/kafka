import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { consumerGroupHeartbeatRequestV1, requestSchema } from './request';

describe('protocol/requests/consumer-group-heartbeat/v1/request', () => {
  it('encodes subscribedTopicRegex after subscribed topic names', async () => {
    const payload = {
      groupId: 'g',
      memberId: 'm',
      memberEpoch: 0,
      instanceId: null as string | null,
      rackId: null as string | null,
      rebalanceTimeoutMs: 60_000,
      subscribedTopicNames: null as string[] | null,
      subscribedTopicRegex: 'events-.*' as string | null,
      serverAssignor: null as string | null,
      topicPartitions: null as { topicId: Buffer; partitions: number[] }[] | null,
    };
    const encoder = await consumerGroupHeartbeatRequestV1(payload).encode();
    const expected = new Encoder()
      .writeUVarIntString('g')
      .writeUVarIntString('m')
      .writeInt32(0)
      .writeUVarIntString(null)
      .writeUVarIntString(null)
      .writeInt32(60_000)
      .writeUVarInt(0)
      .writeUVarIntString('events-.*')
      .writeUVarIntString(null)
      .writeUVarInt(0)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });
});
