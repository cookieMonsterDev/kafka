import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { consumerGroupHeartbeatResponseV0 } from './response';

const topicId = Buffer.from('0123456789abcdef');

function responseFixture(options: {
  errorCode?: number;
  memberId?: string | null;
  memberEpoch?: number;
  heartbeatIntervalMs?: number;
  assignment: Buffer;
}): Buffer {
  return new Encoder()
    .writeInt32(12)
    .writeInt16(options.errorCode ?? 0)
    .writeUVarIntString(null)
    .writeUVarIntString(options.memberId ?? 'member-1')
    .writeInt32(options.memberEpoch ?? 1)
    .writeInt32(options.heartbeatIntervalMs ?? 5_000)
    .writeBuffer(options.assignment)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/consumer-group-heartbeat/v0/response', () => {
  it('decodes a null assignment struct', async () => {
    const data = await consumerGroupHeartbeatResponseV0.decode(
      responseFixture({ assignment: new Encoder().writeInt8(-1).buffer }),
    );

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 12,
      errorCode: 0,
      errorMessage: null,
      memberId: 'member-1',
      memberEpoch: 1,
      heartbeatIntervalMs: 5_000,
      assignment: null,
    });
    await expect(consumerGroupHeartbeatResponseV0.parse(data)).resolves.toBe(data);
  });

  it('decodes an assignment with topic ids and partitions', async () => {
    const assignment = new Encoder()
      .writeInt8(1)
      .writeUVarInt(2)
      .writeBuffer(topicId)
      .writeUVarInt(2)
      .writeInt32(3)
      .writeUVarInt(0)
      .writeUVarInt(0).buffer;

    await expect(consumerGroupHeartbeatResponseV0.decode(responseFixture({ assignment }))).resolves.toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 12,
      errorCode: 0,
      errorMessage: null,
      memberId: 'member-1',
      memberEpoch: 1,
      heartbeatIntervalMs: 5_000,
      assignment: { topicPartitions: [{ topicId, partitions: [3] }] },
    });
  });

  it('rejects a top-level protocol error', async () => {
    const data = await consumerGroupHeartbeatResponseV0.decode(
      responseFixture({ errorCode: 110, assignment: new Encoder().writeInt8(-1).buffer }),
    );
    await expect(consumerGroupHeartbeatResponseV0.parse(data)).rejects.toMatchObject({
      type: 'FENCED_MEMBER_EPOCH',
    });
  });
});
