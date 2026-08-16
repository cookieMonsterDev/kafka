import { describe, expect, it } from 'vitest';
import { KafkaPartialMessageError, KafkaUnsupportedMagicByteInMessageSet } from '../../errors';
import { Decoder } from '../decoder';
import { Encoder } from '../encoder';
import { decodeMessage, encodeMessage } from './index';

describe('protocol/message', () => {
  it('round-trips a v0 message', () => {
    const encoded = encodeMessage(0, { key: 'k', value: 'v' });
    const decoded = decodeMessage(-1n, encoded.size(), new Decoder(encoded.buffer));

    expect(decoded.offset).toBe(-1n);
    expect(decoded.magicByte).toBe(0);
    expect(decoded.key?.toString()).toBe('k');
    expect(decoded.value?.toString()).toBe('v');
    expect(decoded.timestamp).toBeUndefined();
  });

  it('round-trips a v1 message with timestamp', () => {
    const timestamp = 1_509_928_155_660;
    const encoded = encodeMessage(1, { key: 'k', value: 'v', timestamp });
    const decoded = decodeMessage(0n, encoded.size(), new Decoder(encoded.buffer));

    expect(decoded.magicByte).toBe(1);
    expect(decoded.timestamp).toBe(BigInt(timestamp));
    expect(decoded.key?.toString()).toBe('k');
    expect(decoded.value?.toString()).toBe('v');
  });

  it('round-trips null key and value', () => {
    const encoded = encodeMessage(0, { key: null, value: null });
    const decoded = decodeMessage(0n, encoded.size(), new Decoder(encoded.buffer));
    expect(decoded.key).toBeNull();
    expect(decoded.value).toBeNull();
  });

  it('throws KafkaUnsupportedMagicByteInMessageSet for magic > 1', () => {
    const content = new Encoder().writeInt8(2).writeInt8(0).writeBytes('k').writeBytes('v');
    const message = new Encoder().writeInt32(0).writeEncoder(content);

    expect(() => decodeMessage(0n, message.size(), new Decoder(message.buffer))).toThrow(
      KafkaUnsupportedMagicByteInMessageSet,
    );
  });

  it('throws KafkaPartialMessageError when the buffer is shorter than the declared size', () => {
    const encoded = encodeMessage(0, { key: 'k', value: 'v' });
    const truncated = encoded.buffer.subarray(0, 2);

    expect(() => decodeMessage(0n, encoded.size(), new Decoder(truncated))).toThrow(KafkaPartialMessageError);
  });
});
