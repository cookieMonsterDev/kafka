import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { ZERO_TOPIC_ID } from '../shared';
import { metadataRequestV11 } from '../v11/request';
import { metadataRequestV12 } from './request';

const topicId = Buffer.from('0123456789abcdef');

describe('protocol/requests/metadata/v12/request', () => {
  it('matches the v11 body and reports apiVersion 12', async () => {
    const payload = { topics: ['orders'], allowAutoTopicCreation: true };
    const definition = metadataRequestV12(payload);
    expect(definition.apiVersion).toBe(12);

    const encoder = await definition.encode();
    const v11 = await metadataRequestV11(payload).encode();
    expect(encoder.buffer).toEqual(v11.buffer);
  });

  it('encodes topicIds with a null name', async () => {
    const encoder = await metadataRequestV12({
      topics: [],
      topicIds: [topicId],
      allowAutoTopicCreation: true,
    }).encode();

    const expected = new Encoder()
      .writeUVarInt(2)
      .writeBuffer(topicId)
      .writeUVarIntString(null)
      .writeUVarInt(0)
      .writeBoolean(true)
      .writeBoolean(false)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
  });

  it('encodes name-only topics with a zero UUID', async () => {
    const encoder = await metadataRequestV12({ topics: ['orders'], allowAutoTopicCreation: true }).encode();
    expect(encoder.buffer.subarray(1, 17)).toEqual(ZERO_TOPIC_ID);
  });
});
