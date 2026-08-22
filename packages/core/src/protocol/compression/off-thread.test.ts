import { inspect } from 'node:util';
import { describe, expect, it } from 'vitest';
import { runCodecOp } from './off-thread';

describe('protocol/compression/off-thread', () => {
  it('compresses and decompresses snappy off the calling tick', async () => {
    const input = Buffer.from('off-thread snappy');
    const pending = runCodecOp('snappy-compress', input);
    expect(inspect(pending)).toContain('pending');
    const compressed = await pending;
    const roundTrip = await runCodecOp('snappy-decompress', compressed);
    expect(roundTrip).toEqual(input);
  });

  it('compresses and decompresses lz4 off the calling tick', async () => {
    const input = Buffer.from('off-thread lz4');
    const pending = runCodecOp('lz4-compress', input);
    expect(inspect(pending)).toContain('pending');
    const compressed = await pending;
    const roundTrip = await runCodecOp('lz4-decompress', compressed);
    expect(roundTrip).toEqual(input);
  });
});
