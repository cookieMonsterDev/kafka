import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { produceResponseV9 } from './response';

describe('protocol/requests/produce/v9/response', () => {
  it('decodes compact record_errors and remaps throttleTime', async () => {
    const encoded = new Encoder()
      .writeUVarIntArray([
        new Encoder()
          .writeUVarIntString('test-topic')
          .writeUVarIntArray([
            new Encoder()
              .writeInt32(1)
              .writeInt16(87)
              .writeInt64(0n)
              .writeInt64(-1n)
              .writeInt64(0n)
              .writeUVarIntArray([new Encoder().writeInt32(0).writeUVarIntString('record is invalid').writeUVarInt(0)])
              .writeUVarIntString('one or more records failed validation')
              .writeUVarInt(0),
          ])
          .writeUVarInt(0),
      ])
      .writeInt32(20)
      .writeUVarInt(0);

    const data = await produceResponseV9.decode(encoded.buffer);
    expect(data).toEqual({
      topics: [
        {
          topicName: 'test-topic',
          partitions: [
            {
              partition: 1,
              errorCode: 87,
              baseOffset: 0n,
              logAppendTime: -1n,
              logStartOffset: 0n,
              recordErrors: [{ batchIndex: 0, batchIndexErrorMessage: 'record is invalid' }],
              errorMessage: 'one or more records failed validation',
              currentLeader: null,
            },
          ],
        },
      ],
      throttleTime: 0,
      clientSideThrottleTime: 20,
      nodeEndpoints: [],
    });
  });

  it('decodes KIP-951 CurrentLeader and NodeEndpoints tagged fields', async () => {
    const currentLeaderTag = new Encoder().writeInt32(2).writeInt32(7);
    const nodeEndpointsTag = new Encoder().writeUVarIntArray([
      new Encoder()
        .writeInt32(2)
        .writeUVarIntString('broker-2')
        .writeInt32(9092)
        .writeUVarIntString(null)
        .writeUVarInt(0),
    ]);

    const encoded = new Encoder()
      .writeUVarIntArray([
        new Encoder()
          .writeUVarIntString('test-topic')
          .writeUVarIntArray([
            new Encoder()
              .writeInt32(1)
              .writeInt16(6) // NOT_LEADER_OR_FOLLOWER
              .writeInt64(0n)
              .writeInt64(-1n)
              .writeInt64(0n)
              .writeUVarIntArray([])
              .writeUVarIntString(null)
              .writeUVarInt(1) // one tagged field: CurrentLeader
              .writeUVarInt(0) // tag 0
              .writeUVarInt(currentLeaderTag.buffer.length)
              .writeBuffer(currentLeaderTag.buffer),
          ])
          .writeUVarInt(0),
      ])
      .writeInt32(0)
      .writeUVarInt(1) // one top-level tagged field: NodeEndpoints
      .writeUVarInt(0) // tag 0
      .writeUVarInt(nodeEndpointsTag.buffer.length)
      .writeBuffer(nodeEndpointsTag.buffer);

    const data = await produceResponseV9.decode(encoded.buffer);
    expect(data.topics[0]?.partitions[0]?.currentLeader).toEqual({ leaderId: 2, leaderEpoch: 7 });
    expect(data.nodeEndpoints).toEqual([{ nodeId: 2, host: 'broker-2', port: 9092, rack: null }]);
  });
});
