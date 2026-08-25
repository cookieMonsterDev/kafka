import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { fetchResponseV12 } from './response';

describe('protocol/requests/fetch/v12/response', () => {
  it('decodes compact topic names, empty compact records, and tagged fields', async () => {
    const encoded = new Encoder()
      .writeInt32(5)
      .writeInt16(0)
      .writeInt32(42)
      .writeUVarInt(2)
      .writeUVarIntString('orders')
      .writeUVarInt(2)
      .writeInt32(0)
      .writeInt16(0)
      .writeInt64(10n)
      .writeInt64(10n)
      .writeInt64(0n)
      .writeUVarInt(1)
      .writeInt32(-1)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeUVarInt(0);

    const decoded = await fetchResponseV12().decode(encoded.buffer);
    expect(decoded).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 5,
      errorCode: 0,
      sessionId: 42,
      responses: [
        {
          topicName: 'orders',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              highWatermark: 10n,
              lastStableOffset: 10n,
              logStartOffset: 0n,
              abortedTransactions: [],
              preferredReadReplica: -1,
              messages: [],
              currentLeader: null,
            },
          ],
        },
      ],
      nodeEndpoints: [],
    });
  });

  it('decodes KIP-951 CurrentLeader (partition tag 1) and NodeEndpoints (response tag 0)', async () => {
    const currentLeaderTag = new Encoder().writeInt32(3).writeInt32(4);
    const nodeEndpointsTag = new Encoder().writeUVarIntArray([
      new Encoder()
        .writeInt32(3)
        .writeUVarIntString('broker-3')
        .writeInt32(9094)
        .writeUVarIntString('rack-a')
        .writeUVarInt(0),
    ]);

    const encoded = new Encoder()
      .writeInt32(0)
      .writeInt16(0)
      .writeInt32(0)
      .writeUVarInt(2)
      .writeUVarIntString('orders')
      .writeUVarInt(2)
      .writeInt32(0)
      .writeInt16(6) // NOT_LEADER_OR_FOLLOWER
      .writeInt64(0n)
      .writeInt64(0n)
      .writeInt64(0n)
      .writeUVarInt(1)
      .writeInt32(-1)
      .writeUVarInt(0)
      .writeUVarInt(1) // partition-level TAG_BUFFER: one tagged field
      .writeUVarInt(1) // tag 1 (CurrentLeader)
      .writeUVarInt(currentLeaderTag.buffer.length)
      .writeBuffer(currentLeaderTag.buffer)
      .writeUVarInt(0) // topic-level TAG_BUFFER
      .writeUVarInt(1) // top-level TAG_BUFFER: one tagged field
      .writeUVarInt(0) // tag 0 (NodeEndpoints)
      .writeUVarInt(nodeEndpointsTag.buffer.length)
      .writeBuffer(nodeEndpointsTag.buffer);

    const decoded = await fetchResponseV12().decode(encoded.buffer);
    expect(decoded.responses[0]?.partitions[0]?.currentLeader).toEqual({ leaderId: 3, leaderEpoch: 4 });
    expect(decoded.nodeEndpoints).toEqual([{ nodeId: 3, host: 'broker-3', port: 9094, rack: 'rack-a' }]);
  });
});
