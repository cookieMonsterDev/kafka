import { describe, expect, it } from 'vitest';
import { crc32 } from './crc32';
import { Encoder } from './encoder';

describe('protocol/crc32', () => {
  it('matches the standard CRC-32/ISO-HDLC check value for "123456789"', () => {
    // https://reveng.sourceforge.io/crc-catalogue/17plus.htm#crc.cat.crc-32-iso-hdlc — check=0xcbf43926
    expect(crc32(Buffer.from('123456789'))).toBe(0xcbf43926 | 0);
  });

  it('accepts an Encoder and hashes only the written bytes', () => {
    const encoder = new Encoder().writeBytes(Buffer.from('123456789'));
    expect(crc32(encoder)).toBe(crc32(encoder.buffer));
  });

  it('is sensitive to every byte', () => {
    const a = Buffer.from('kafka-core');
    const b = Buffer.from('kafka-core');
    b[0] = (b[0] as number) + 1;
    expect(crc32(a)).not.toBe(crc32(b));
  });
});
