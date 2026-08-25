import { describe, expect, it } from 'vitest';
import { KafkaCorruptRecordError } from '../../errors';
import { Decoder } from '../decoder';
import { Encoder } from '../encoder';
import { decodeMessageSet } from './decoder';
import { encodeMessageSet } from './index';
import uncompressedFixture from './fixtures/messages-v0-uncompressed.json' with { type: 'json' };
import gzipFixture from './fixtures/messages-v0-gzip.json' with { type: 'json' };

const v0Messages = [0, 1, 2].map((i) => ({
  offset: BigInt(i),
  size: 31,
  crc: [120234579, -141862522, 1025004472][i]!,
  magicByte: 0,
  attributes: 0,
  timestamp: 0n,
  key: Buffer.from(`key-${i}`),
  value: Buffer.from(`some-value-${i}`),
  headers: {},
  isControlRecord: false,
  batchContext: expect.objectContaining({
    magicByte: 0,
    firstOffset: BigInt(i),
    inTransaction: false,
    producerId: -1n,
  }),
  byteSize: 31,
}));

describe('protocol/message-set/decoder', () => {
  it('decodes an uncompressed v0 fixture', async () => {
    const decoder = new Decoder(Buffer.from(uncompressedFixture.data));
    await expect(decodeMessageSet(decoder)).resolves.toEqual(v0Messages);
  });

  it('decodes a gzip-compressed v0 fixture into the inner messages', async () => {
    const decoder = new Decoder(Buffer.from(gzipFixture.data));
    await expect(decodeMessageSet(decoder)).resolves.toEqual(v0Messages);
  });

  it('skips a truncated trailing message', async () => {
    const messageSet = encodeMessageSet({
      messageVersion: 0,
      entries: [{ key: 'v0-key', value: 'v0-value' }],
    });

    const truncated = messageSet.buffer.subarray(Decoder.int32Size());
    const decoder = new Decoder(new Encoder().writeInt32(messageSet.size()).writeBuffer(truncated).buffer);

    await expect(decodeMessageSet(decoder)).resolves.toEqual([]);
  });

  it('throws when a compressed entry has a null value', async () => {
    const { encodeMessageV0 } = await import('../message/v0');
    const { COMPRESSION_TYPES } = await import('../compression/index');
    const message = encodeMessageV0({ compression: COMPRESSION_TYPES.GZIP, key: null, value: null });
    const set = new Encoder().writeInt64(-1n).writeInt32(message.size()).writeEncoder(message);
    const decoder = new Decoder(new Encoder().writeInt32(set.size()).writeEncoder(set).buffer);
    await expect(decodeMessageSet(decoder)).rejects.toThrow('null value');
  });

  describe('checkCrcs', () => {
    function corruptedMessageSetBuffer(): Buffer {
      const messageSet = encodeMessageSet({ messageVersion: 0, entries: [{ key: 'k', value: 'hello' }] });
      const corrupted = Buffer.from(messageSet.buffer);
      corrupted[corrupted.length - 1] = (corrupted[corrupted.length - 1] as number) ^ 0xff;
      return corrupted;
    }

    it('defaults to true and rejects a message whose CRC does not match its bytes', async () => {
      const corrupted = corruptedMessageSetBuffer();
      const decoder = new Decoder(new Encoder().writeInt32(corrupted.length).writeBuffer(corrupted).buffer);
      await expect(decodeMessageSet(decoder)).rejects.toBeInstanceOf(KafkaCorruptRecordError);
    });

    it('checkCrcs: false skips the check and decodes the corrupted message anyway', async () => {
      const corrupted = corruptedMessageSetBuffer();
      const decoder = new Decoder(new Encoder().writeInt32(corrupted.length).writeBuffer(corrupted).buffer);
      const messages = await decodeMessageSet(decoder, undefined, false);
      expect(messages).toHaveLength(1);
    });
  });
});
