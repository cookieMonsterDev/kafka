import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { fetchResponseV14 } from './response';

const topicId = Buffer.from('0123456789abcdef');

describe('protocol/requests/fetch/v14/response', () => {
  it('decodes topicId like v13', async () => {
    const encoded = new Encoder()
      .writeInt32(0)
      .writeInt16(0)
      .writeInt32(0)
      .writeUVarInt(2)
      .writeBuffer(topicId)
      .writeUVarInt(1)
      .writeUVarInt(0)
      .writeUVarInt(0).buffer;

    const decoded = await fetchResponseV14({
      topics: [{ topic: 'orders', topicId, partitions: [] }],
    }).decode(encoded);

    expect(decoded.responses[0]?.topicName).toBe('orders');
    expect(decoded.responses[0]?.topicId).toEqual(topicId);
    expect(decoded.responses[0]?.partitions).toEqual([]);
  });
});
