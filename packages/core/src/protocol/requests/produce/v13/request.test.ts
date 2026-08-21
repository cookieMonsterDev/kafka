import { describe, expect, it } from 'vitest';
import { COMPRESSION_TYPES } from '../../../compression/index';
import { Decoder } from '../../../decoder';
import { produceRequestV12 } from '../v12/request';
import { produceRequestV13 } from './request';
import type { ProduceRequestOptions } from '../shared';

const topicId = Buffer.from('0123456789abcdef');

const args: ProduceRequestOptions = {
  acks: -1,
  timeout: 30000,
  transactionalId: null,
  producerId: -1n,
  producerEpoch: 0,
  compression: COMPRESSION_TYPES.None,
  topicData: [
    {
      topic: 'test-topic',
      topicId,
      partitions: [
        {
          partition: 0,
          firstSequence: 0,
          messages: [{ key: 'k', value: 'v', timestamp: 1509928155660 }],
        },
      ],
    },
  ],
};

describe('protocol/requests/produce/v13/request', () => {
  it('sets expectResponse to false when acks=0', () => {
    expect(produceRequestV13({ ...args, acks: 0 }).expectResponse?.()).toBe(false);
  });

  it('encodes a topic UUID instead of a compact topic name', async () => {
    const v12 = await produceRequestV12(args).encode();
    const v13 = await produceRequestV13(args).encode();

    expect(produceRequestV13(args).apiVersion).toBe(13);
    expect(v13.buffer).not.toEqual(v12.buffer);

    const decoder = new Decoder(v13.buffer);
    expect(decoder.readUVarIntString()).toBeNull();
    expect(decoder.readInt16()).toBe(-1);
    expect(decoder.readInt32()).toBe(30000);
    expect(decoder.readUVarInt() - 1).toBe(1);
    expect(decoder.readBytes(16)).toEqual(topicId);
    expect(decoder.readUVarInt() - 1).toBe(1);
    expect(decoder.readInt32()).toBe(0);
    expect(decoder.readUVarIntBytes()).not.toBeNull();
    expect(decoder.readUVarInt()).toBe(0);
    expect(decoder.readUVarInt()).toBe(0);
    expect(decoder.readUVarInt()).toBe(0);
  });

  it('rejects a request that has no usable topicId', async () => {
    const request = produceRequestV13({
      ...args,
      topicData: [{ topic: 'test-topic', partitions: [{ partition: 0, firstSequence: 0, messages: [] }] }],
    });
    await expect(request.encode()).rejects.toThrow(/requires a 16-byte topicId/);
  });
});
