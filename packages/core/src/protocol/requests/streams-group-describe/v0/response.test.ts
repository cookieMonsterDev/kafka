import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { streamsGroupDescribeResponseV0 } from './response';

function taskIds(subtopologyId: string, partitions: number[]): Encoder {
  const encoder = new Encoder().writeUVarIntString(subtopologyId).writeUVarInt(partitions.length + 1);
  for (const partition of partitions) encoder.writeInt32(partition);
  return encoder.writeUVarInt(0);
}

function assignment(): Encoder {
  return new Encoder()
    .writeUVarInt(2)
    .writeBuffer(taskIds('0', [0, 1]).buffer)
    .writeUVarInt(2)
    .writeBuffer(taskIds('1', [0]).buffer)
    .writeUVarInt(1)
    .writeUVarInt(0);
}

function member(): Encoder {
  return new Encoder()
    .writeUVarIntString('member-1')
    .writeInt32(5)
    .writeUVarIntString(null)
    .writeUVarIntString('rack-a')
    .writeUVarIntString('client')
    .writeUVarIntString('/127.0.0.1')
    .writeInt32(2)
    .writeUVarIntString('process-1')
    .writeInt8(1)
    .writeUVarIntString('localhost')
    .writeUInt16(65000)
    .writeUVarInt(0)
    .writeUVarInt(2)
    .writeUVarIntString('zone')
    .writeUVarIntString('a')
    .writeUVarInt(0)
    .writeUVarInt(2)
    .writeUVarIntString('0')
    .writeInt32(1)
    .writeInt64(10n)
    .writeUVarInt(0)
    .writeUVarInt(1)
    .writeBuffer(assignment().buffer)
    .writeBuffer(assignment().buffer)
    .writeBoolean(false)
    .writeUVarInt(0);
}

function topology(): Encoder {
  return new Encoder()
    .writeInt8(1)
    .writeInt32(2)
    .writeUVarInt(2)
    .writeUVarIntString('0')
    .writeUVarInt(2)
    .writeUVarIntString('orders')
    .writeUVarInt(2)
    .writeUVarIntString('app-repartition')
    .writeUVarInt(2)
    .writeUVarIntString('app-store-changelog')
    .writeInt32(0)
    .writeInt16(0)
    .writeUVarInt(2)
    .writeUVarIntString('cleanup.policy')
    .writeUVarIntString('compact')
    .writeUVarInt(0)
    .writeUVarInt(0)
    .writeUVarInt(1)
    .writeUVarInt(0)
    .writeUVarInt(0);
}

const expectedAssignment = {
  activeTasks: [{ subtopologyId: '0', partitions: [0, 1] }],
  standbyTasks: [{ subtopologyId: '1', partitions: [0] }],
  warmupTasks: [],
};

describe('protocol/requests/streams-group-describe/v0/response', () => {
  it('decodes a described streams group with its topology and member', async () => {
    const buffer = new Encoder()
      .writeInt32(7)
      .writeUVarInt(2)
      .writeInt16(0)
      .writeUVarIntString(null)
      .writeUVarIntString('app')
      .writeUVarIntString('Stable')
      .writeInt32(4)
      .writeInt32(4)
      .writeBuffer(topology().buffer)
      .writeUVarInt(2)
      .writeBuffer(member().buffer)
      .writeInt32(-2147483648)
      .writeUVarInt(0)
      .writeUVarInt(0).buffer;

    const data = await streamsGroupDescribeResponseV0.decode(buffer);
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 7,
      groups: [
        {
          errorCode: 0,
          errorMessage: null,
          groupId: 'app',
          groupState: 'Stable',
          groupEpoch: 4,
          assignmentEpoch: 4,
          topology: {
            epoch: 2,
            subtopologies: [
              {
                subtopologyId: '0',
                sourceTopics: ['orders'],
                repartitionSinkTopics: ['app-repartition'],
                stateChangelogTopics: [
                  {
                    name: 'app-store-changelog',
                    partitions: 0,
                    replicationFactor: 0,
                    topicConfigs: [{ key: 'cleanup.policy', value: 'compact' }],
                  },
                ],
                repartitionSourceTopics: [],
              },
            ],
          },
          members: [
            {
              memberId: 'member-1',
              memberEpoch: 5,
              instanceId: null,
              rackId: 'rack-a',
              clientId: 'client',
              clientHost: '/127.0.0.1',
              topologyEpoch: 2,
              processId: 'process-1',
              userEndpoint: { host: 'localhost', port: 65000 },
              clientTags: [{ key: 'zone', value: 'a' }],
              taskOffsets: [{ subtopologyId: '0', partition: 1, offset: 10n }],
              taskEndOffsets: [],
              assignment: expectedAssignment,
              targetAssignment: expectedAssignment,
              isClassic: false,
            },
          ],
          authorizedOperations: -2147483648,
          topologyDescription: null,
          topologyDescriptionStatus: 0,
          assignorName: null,
        },
      ],
    });
    await expect(streamsGroupDescribeResponseV0.parse(data)).resolves.toBe(data);
  });

  it('decodes a null topology, null subtopologies and a member without an endpoint', async () => {
    const memberWithoutEndpoint = new Encoder()
      .writeUVarIntString('member-1')
      .writeInt32(0)
      .writeUVarIntString('instance-1')
      .writeUVarIntString(null)
      .writeUVarIntString('client')
      .writeUVarIntString('/127.0.0.1')
      .writeInt32(-1)
      .writeUVarIntString('process-1')
      .writeInt8(-1)
      .writeUVarInt(1)
      .writeUVarInt(1)
      .writeUVarInt(1)
      .writeUVarInt(1)
      .writeUVarInt(1)
      .writeUVarInt(1)
      .writeUVarInt(0)
      .writeUVarInt(1)
      .writeUVarInt(1)
      .writeUVarInt(1)
      .writeUVarInt(0)
      .writeBoolean(true)
      .writeUVarInt(0);
    const group = (topologyBytes: Encoder) =>
      new Encoder()
        .writeInt16(0)
        .writeUVarIntString(null)
        .writeUVarIntString('app')
        .writeUVarIntString('NotReady')
        .writeInt32(1)
        .writeInt32(0)
        .writeBuffer(topologyBytes.buffer)
        .writeUVarInt(2)
        .writeBuffer(memberWithoutEndpoint.buffer)
        .writeInt32(-2147483648)
        .writeUVarInt(0);
    const buffer = new Encoder()
      .writeInt32(0)
      .writeUVarInt(3)
      .writeBuffer(group(new Encoder().writeInt8(-1)).buffer)
      .writeBuffer(group(new Encoder().writeInt8(1).writeInt32(1).writeUVarInt(0).writeUVarInt(0)).buffer)
      .writeUVarInt(0).buffer;

    const data = await streamsGroupDescribeResponseV0.decode(buffer);
    expect(data.groups.map((described) => described.topology)).toEqual([null, { epoch: 1, subtopologies: null }]);
    expect(data.groups[0]?.members[0]).toMatchObject({
      instanceId: 'instance-1',
      userEndpoint: null,
      isClassic: true,
      assignment: { activeTasks: [], standbyTasks: [], warmupTasks: [] },
    });
  });

  it('rejects a per-group protocol error with the broker message', async () => {
    const buffer = new Encoder()
      .writeInt32(0)
      .writeUVarInt(2)
      .writeInt16(69)
      .writeUVarIntString('Group missing not found.')
      .writeUVarIntString('missing')
      .writeUVarIntString('')
      .writeInt32(0)
      .writeInt32(0)
      .writeInt8(-1)
      .writeUVarInt(1)
      .writeInt32(-2147483648)
      .writeUVarInt(0)
      .writeUVarInt(0).buffer;

    const data = await streamsGroupDescribeResponseV0.decode(buffer);
    await expect(streamsGroupDescribeResponseV0.parse(data)).rejects.toMatchObject({
      type: 'GROUP_ID_NOT_FOUND',
      message: 'Group missing not found.',
    });
  });
});
