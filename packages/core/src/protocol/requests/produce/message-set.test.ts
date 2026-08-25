import { describe, expect, it } from 'vitest';
import { COMPRESSION_TYPES } from '../../compression/index';
import { Decoder } from '../../decoder';
import { Encoder } from '../../encoder';
import { gzipCodec } from '../../compression/gzip';
import { decodeMessageSet } from '../../message-set/decoder';
import { createMessageSetProduceRequest } from './message-set';

function prefixed(messageSet: Encoder): Decoder {
  return new Decoder(new Encoder().writeInt32(messageSet.size()).writeEncoder(messageSet).buffer);
}

describe('protocol/requests/produce/message-set', () => {
  it('does not expect a response when acks is 0', () => {
    const request = createMessageSetProduceRequest(0, {
      acks: 0,
      timeout: 1000,
      topicData: [{ topic: 't', partitions: [{ partition: 0, messages: [{ value: 'a' }] }] }],
    });
    expect(request.expectResponse?.()).toBe(false);
    expect(request.apiVersion).toBe(0);
  });

  it('encodes an uncompressed v0 MessageSet produce request', async () => {
    const encoded = await createMessageSetProduceRequest(0, {
      acks: 1,
      timeout: 1000,
      topicData: [{ topic: 'orders', partitions: [{ partition: 0, messages: [{ key: 'k', value: 'v' }] }] }],
    }).encode();

    const decoder = new Decoder(encoded.buffer);
    expect(decoder.readInt16()).toBe(1);
    expect(decoder.readInt32()).toBe(1000);
    expect(decoder.readInt32()).toBe(1);
    expect(decoder.readString()).toBe('orders');
    expect(decoder.readInt32()).toBe(1);
    expect(decoder.readInt32()).toBe(0);
    const recordSet = decoder.readBytes();
    expect(recordSet).not.toBeNull();
    const messages = await decodeMessageSet(prefixed(new Encoder().writeBuffer(recordSet!)));
    expect(messages).toHaveLength(1);
    expect(messages[0]?.key?.toString()).toBe('k');
    expect(messages[0]?.value?.toString()).toBe('v');
    expect(messages[0]?.magicByte).toBe(0);
  });

  it('wraps a gzip-compressed magic-1 MessageSet on Produce v2', async () => {
    const encoded = await createMessageSetProduceRequest(2, {
      acks: 1,
      timeout: 1000,
      compression: COMPRESSION_TYPES.GZIP,
      topicData: [
        {
          topic: 'orders',
          partitions: [
            {
              partition: 0,
              messages: [
                { key: 'k0', value: 'v0', timestamp: 1_000 },
                { key: 'k1', value: 'v1', timestamp: 1_000 },
              ],
            },
          ],
        },
      ],
    }).encode();

    const decoder = new Decoder(encoded.buffer);
    decoder.readInt16();
    decoder.readInt32();
    decoder.readInt32();
    decoder.readString();
    decoder.readInt32();
    decoder.readInt32();
    const recordSet = decoder.readBytes();
    const messages = await decodeMessageSet(prefixed(new Encoder().writeBuffer(recordSet!)));
    expect(messages).toHaveLength(2);
    expect(messages.map((message) => message.key?.toString())).toEqual(['k0', 'k1']);
    expect(messages[0]?.magicByte).toBe(1);
    expect(messages[0]?.timestamp).toBe(1000n);
  });

  it('throws when no codec is registered for the compression type', async () => {
    await expect(
      createMessageSetProduceRequest(2, {
        acks: 1,
        timeout: 1000,
        compression: 99 as never,
        topicData: [{ topic: 't', partitions: [{ partition: 0, messages: [{ value: 'a' }] }] }],
      }).encode(),
    ).rejects.toThrow('no codec registered');
  });
});

describe('protocol/message-set compressed wrapper offsets', () => {
  it('rebases inner offsets onto the compressed wrapper offset', async () => {
    const { encodeMessageSet } = await import('../../message-set/index');
    const innerSet = encodeMessageSet({
      messageVersion: 1,
      compression: COMPRESSION_TYPES.GZIP,
      entries: [
        { key: 'a', value: '0', timestamp: 10 },
        { key: 'b', value: '1', timestamp: 10 },
      ],
    });
    const compressed = await gzipCodec.compress(innerSet);
    const wrapper = encodeMessageSet({
      messageVersion: 1,
      entries: [{ compression: COMPRESSION_TYPES.GZIP, timestamp: 10, value: compressed }],
    });
    const buffer = Buffer.from(wrapper.buffer);
    buffer.writeBigInt64BE(100n, 0);
    const messages = await decodeMessageSet(prefixed(new Encoder().writeBuffer(buffer)));
    expect(messages.map((message) => message.offset)).toEqual([99n, 100n]);
  });
});
