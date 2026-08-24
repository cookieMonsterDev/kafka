import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { fetchResponseV16 } from './response';

const topicId = Buffer.from('0123456789abcdef');

describe('protocol/requests/fetch/v16/response', () => {
  it('decodes tagged NodeEndpoints (KIP-951) and restores topicName', async () => {
    const nodeEndpointsTag = new Encoder().writeUVarIntArray([
      new Encoder()
        .writeInt32(9)
        .writeUVarIntString('broker-9')
        .writeInt32(9099)
        .writeUVarIntString(null)
        .writeUVarInt(0),
    ]);

    const encoded = new Encoder()
      .writeInt32(0)
      .writeInt16(0)
      .writeInt32(0)
      .writeUVarInt(2)
      .writeBuffer(topicId)
      .writeUVarInt(1)
      .writeUVarInt(0)
      .writeUVarInt(1)
      .writeUVarInt(0)
      .writeUVarInt(nodeEndpointsTag.buffer.length)
      .writeBuffer(nodeEndpointsTag.buffer).buffer;

    const decoded = await fetchResponseV16({
      topics: [{ topic: 'orders', topicId, partitions: [] }],
    }).decode(encoded);

    expect(decoded.responses[0]?.topicName).toBe('orders');
    expect(decoded.responses[0]?.topicId).toEqual(topicId);
    expect(decoded.nodeEndpoints).toEqual([{ nodeId: 9, host: 'broker-9', port: 9099, rack: null }]);
  });
});
