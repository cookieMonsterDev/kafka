// Based on the kafka client 0.10.2 murmur2 implementation
// https://github.com/apache/kafka/blob/0.10.2/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L364

// 'm' and 'r' are mixing constants generated offline. They're not really 'magic', they just
// happen to work well.
const SEED = 0x9747b28c;
const M = 0x5bd1e995;
const R = 24;

function byteAt(data: Buffer, index: number): number {
  return data[index] ?? 0;
}

/**
 * This is the pre-2.0.0 partitioner's hash, kept bit-for-bit identical to kafkajs's own
 * `legacy/murmur2.js` on purpose - `LegacyPartitioner` exists solely so callers upgrading from an
 * old kafkajs can keep routing keys to the exact same partitions their existing consumers expect,
 * and that means preserving two accidental quirks the "fixed" `default` hash (`../default/murmur2.js`)
 * does not have:
 *
 *  - Multiplication uses plain `*`, not a 32-bit-correct multiply. Once an intermediate product
 *    exceeds 2^53 this loses precision that a real 32-bit multiply would not, so the result is
 *    genuinely different from (and not simply "less correct than") the default hash.
 *  - The loop bound is `length / 4`, not `Math.floor(length / 4)`. For an input whose length isn't
 *    a multiple of 4, this lets the loop run one extra iteration and read past the end of `data` -
 *    `byteAt` returns 0 for those out-of-range reads, mirroring `undefined & 0xff` in the original.
 */
export function murmur2(key: Buffer | string | number): number {
  const data = Buffer.isBuffer(key) ? key : Buffer.from(String(key));
  const length = data.length;

  let h = SEED ^ length;
  const length4 = length / 4;

  for (let i = 0; i < length4; i++) {
    const i4 = i * 4;
    let k =
      byteAt(data, i4) + (byteAt(data, i4 + 1) << 8) + (byteAt(data, i4 + 2) << 16) + (byteAt(data, i4 + 3) << 24);

    k *= M;
    k ^= k >>> R;
    k *= M;
    h *= M;
    h ^= k;
  }

  // Handle the last few bytes of the input array. Written as a cascading if (rather than kafkajs's
  // own fallthrough switch) so a length%4 of 3 runs all three blocks, 2 runs the last two, and 1
  // runs only the last - same intentional fallthrough, without tripping `noFallthroughCasesInSwitch`.
  const remainder = length % 4;
  if (remainder >= 3) {
    h ^= byteAt(data, (length & ~3) + 2) << 16;
  }
  if (remainder >= 2) {
    h ^= byteAt(data, (length & ~3) + 1) << 8;
  }
  if (remainder >= 1) {
    h ^= byteAt(data, length & ~3);
    h *= M;
  }

  h ^= h >>> 13;
  h *= M;
  h ^= h >>> 15;

  return h;
}
