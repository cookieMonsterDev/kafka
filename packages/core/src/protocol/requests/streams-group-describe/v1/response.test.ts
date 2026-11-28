import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { streamsGroupDescribeResponseV1 } from './response';

function node(
  name: string,
  nodeType: number,
  { sourceTopics = [], sinkTopic = null }: { sourceTopics?: string[]; sinkTopic?: string | null } = {},
): Encoder {
  const encoder = new Encoder()
    .writeUVarIntString(name)
    .writeInt8(nodeType)
    .writeUVarInt(sourceTopics.length + 1);
  for (const topic of sourceTopics) encoder.writeUVarIntString(topic);
  return encoder.writeUVarIntString(sinkTopic).writeUVarInt(1).writeUVarInt(1).writeUVarInt(0);
}

function group({
  topologyDescription,
  status,
  assignorName,
}: {
  topologyDescription: Encoder;
  status: number;
  assignorName: string | null;
}): Encoder {
  return new Encoder()
    .writeInt16(0)
    .writeUVarIntString(null)
    .writeUVarIntString('app')
    .writeUVarIntString('Empty')
    .writeInt32(1)
    .writeInt32(1)
    .writeInt8(-1)
    .writeUVarInt(1)
    .writeInt32(-2147483648)
    .writeBuffer(topologyDescription.buffer)
    .writeInt8(status)
    .writeUVarIntString(assignorName)
    .writeUVarInt(0);
}

describe('protocol/requests/streams-group-describe/v1/response', () => {
  it('decodes the topology description, its status and the assignor name', async () => {
    const description = new Encoder()
      .writeInt8(1)
      .writeUVarInt(2)
      .writeUVarIntString('0')
      .writeUVarInt(3)
      .writeBuffer(node('KSTREAM-SOURCE-0000000000', 1, { sourceTopics: ['orders'] }).buffer)
      .writeBuffer(node('KSTREAM-SINK-0000000001', 3, { sinkTopic: 'totals' }).buffer)
      .writeUVarInt(0)
      .writeUVarInt(2)
      .writeBuffer(node('KSTREAM-SOURCE-0000000002', 1, { sourceTopics: ['rates'] }).buffer)
      .writeBuffer(node('KTABLE-SOURCE-0000000003', 2).buffer)
      .writeUVarInt(0)
      .writeUVarInt(0);
    const buffer = new Encoder()
      .writeInt32(0)
      .writeUVarInt(3)
      .writeBuffer(group({ topologyDescription: description, status: 3, assignorName: 'sticky' }).buffer)
      .writeBuffer(group({ topologyDescription: new Encoder().writeInt8(-1), status: 1, assignorName: null }).buffer)
      .writeUVarInt(0).buffer;

    const data = await streamsGroupDescribeResponseV1.decode(buffer);
    const emptyNode = { sourceTopics: [], sinkTopic: null, stores: [], successors: [] };
    expect(data.groups[0]).toMatchObject({
      groupId: 'app',
      topology: null,
      members: [],
      topologyDescriptionStatus: 3,
      assignorName: 'sticky',
      topologyDescription: {
        subtopologies: [
          {
            subtopologyId: '0',
            nodes: [
              { ...emptyNode, name: 'KSTREAM-SOURCE-0000000000', nodeType: 1, sourceTopics: ['orders'] },
              { ...emptyNode, name: 'KSTREAM-SINK-0000000001', nodeType: 3, sinkTopic: 'totals' },
            ],
          },
        ],
        globalStores: [
          {
            source: { ...emptyNode, name: 'KSTREAM-SOURCE-0000000002', nodeType: 1, sourceTopics: ['rates'] },
            processor: { ...emptyNode, name: 'KTABLE-SOURCE-0000000003', nodeType: 2 },
          },
        ],
      },
    });
    expect(data.groups[1]).toMatchObject({
      topologyDescription: null,
      topologyDescriptionStatus: 1,
      assignorName: null,
    });
    await expect(streamsGroupDescribeResponseV1.parse(data)).resolves.toBe(data);
  });
});
