import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { encodeMessageSet } from '../../../message-set/index';
import { produceRequestV0 } from './request';

describe('protocol/requests/produce/v0/request', () => {
  const topicData = [
    {
      topic: 'test-topic-1',
      partitions: [
        {
          partition: 0,
          messages: [
            { key: '1', value: 'value-1' },
            { key: '2', value: 'value-2' },
          ],
        },
        { partition: 1, messages: [{ key: '3', value: 'value-3' }] },
      ],
    },
  ];

  it('sets expectResponse to false when acks=0', () => {
    const request = produceRequestV0({ acks: 0, timeout: 1000, topicData });
    expect(request.expectResponse?.()).toBe(false);
  });

  it('encodes acks, timeout, and MessageSet magic 0 partitions', async () => {
    const encoder = await produceRequestV0({ acks: -1, timeout: 1000, topicData }).encode();
    const ms1 = encodeMessageSet({ entries: topicData[0]!.partitions[0]!.messages });
    const ms2 = encodeMessageSet({ entries: topicData[0]!.partitions[1]!.messages });

    const expected = new Encoder()
      .writeInt16(-1)
      .writeInt32(1000)
      .writeArray([
        new Encoder()
          .writeString('test-topic-1')
          .writeArray([
            new Encoder().writeInt32(0).writeInt32(ms1.size()).writeEncoder(ms1),
            new Encoder().writeInt32(1).writeInt32(ms2.size()).writeEncoder(ms2),
          ]),
      ]);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(produceRequestV0({ acks: -1, timeout: 1000, topicData }).apiVersion).toBe(0);
  });
});
