import { describe, expect, it } from 'vitest';
import { COMPRESSION_TYPES } from '../../../compression/index';
import { Decoder } from '../../../decoder';
import { produceRequestV8 } from '../v8/request';
import { produceRequestV9 } from './request';
import type { ProduceRequestOptions } from '../shared';

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

describe('protocol/requests/produce/v9/request', () => {
  it('sets expectResponse to false when acks=0', () => {
    expect(produceRequestV9({ ...args, acks: 0 }).expectResponse?.()).toBe(false);
  });

  it('encodes compact nullable strings and compact arrays, not the v8 length prefixes', async () => {
    const v8 = await produceRequestV8(args).encode();
    const v9 = await produceRequestV9(args).encode();

    expect(produceRequestV9(args).apiVersion).toBe(9);
    // v8 nullable STRING null is int16 -1; v9 compact nullable STRING null is uvarint 0.
    expect(v8.buffer.subarray(0, 2)).toEqual(Buffer.from([0xff, 0xff]));
    expect(v9.buffer[0]).toBe(0);
    expect(v9.buffer).not.toEqual(v8.buffer);

    const decoder = new Decoder(v9.buffer);
    expect(decoder.readUVarIntString()).toBeNull();
    expect(decoder.readInt16()).toBe(-1);
    expect(decoder.readInt32()).toBe(30000);
    expect(decoder.readUVarInt() - 1).toBe(1);
    expect(decoder.readUVarIntString()).toBe('test-topic');
    expect(decoder.readUVarInt() - 1).toBe(1);
    expect(decoder.readInt32()).toBe(0);
    expect(decoder.readUVarIntBytes()).not.toBeNull();
    expect(decoder.readUVarInt()).toBe(0);
    expect(decoder.readUVarInt()).toBe(0);
    expect(decoder.readUVarInt()).toBe(0);
  });
});
