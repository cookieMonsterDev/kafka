import { describe, expect, it } from 'vitest';
import { COMPRESSION_TYPES } from '../compression/index';
import { Decoder } from '../decoder';
import { Encoder } from '../encoder';
import { decodeMessageSet } from './decoder';
import { encodeMessageSet } from './index';

function prefixed(messageSet: Encoder): Decoder {
  return new Decoder(new Encoder().writeInt32(messageSet.size()).writeEncoder(messageSet).buffer);
}

describe('protocol/message-set', () => {
  it('round-trips uncompressed magic 0 messages with bigint offsets', async () => {
    const encoded = encodeMessageSet({
      messageVersion: 0,
      entries: [
        { key: 'key-0', value: 'value-0' },
        { key: 'key-1', value: 'value-1' },
      ],
    });

    const decoded = await decodeMessageSet(prefixed(encoded));

    expect(decoded).toHaveLength(2);
    expect(decoded[0]?.offset).toBe(-1n);
    expect(decoded[0]?.magicByte).toBe(0);
    expect(decoded[0]?.key?.toString()).toBe('key-0');
    expect(decoded[0]?.value?.toString()).toBe('value-0');
    expect(decoded[0]?.headers).toEqual({});
    expect(decoded[0]?.timestamp).toBe(0n);
    expect(typeof decoded[0]?.offset).toBe('bigint');
    expect(decoded[1]?.key?.toString()).toBe('key-1');
  });

  it('round-trips a gzip inner set with magic 1 timestamps', async () => {
    const timestamp = 1_509_928_155_660;
    const encoded = encodeMessageSet({
      messageVersion: 1,
      compression: COMPRESSION_TYPES.GZIP,
      entries: [
        { key: 'k0', value: 'v0', timestamp },
        { key: 'k1', value: 'v1', timestamp },
      ],
    });

    const decoded = await decodeMessageSet(prefixed(encoded));

    expect(decoded).toHaveLength(2);
    expect(decoded[0]?.magicByte).toBe(1);
    expect(decoded[0]?.offset).toBe(0n);
    expect(decoded[1]?.offset).toBe(1n);
    expect(decoded[0]?.timestamp).toBe(BigInt(timestamp));
    expect(decoded[0]?.key?.toString()).toBe('k0');
    expect(decoded[1]?.value?.toString()).toBe('v1');
  });

  it('stops on a truncated trailing message instead of throwing', async () => {
    const encoded = encodeMessageSet({
      messageVersion: 0,
      entries: [
        { key: 'key-0', value: 'value-0' },
        { key: 'key-1', value: 'value-1' },
      ],
    });
    const truncated = encoded.buffer.subarray(0, encoded.size() - 4);
    const decoded = await decodeMessageSet(prefixed(new Encoder().writeBuffer(truncated)));

    expect(decoded.length).toBeGreaterThanOrEqual(1);
    expect(decoded[0]?.key?.toString()).toBe('key-0');
  });

  it('stops when a RecordBatch magic byte appears in a MessageSet', async () => {
    const content = new Encoder().writeInt8(2).writeInt8(0).writeBytes('k').writeBytes('v');
    const message = new Encoder().writeInt32(0).writeEncoder(content);
    const set = new Encoder().writeInt64(-1n).writeInt32(message.size()).writeEncoder(message);
    const decoded = await decodeMessageSet(prefixed(set));

    expect(decoded).toEqual([]);
  });
});
