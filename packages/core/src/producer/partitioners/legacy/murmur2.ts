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
 * Pre-2.0 murmur2 used by {@link LegacyPartitioner}. Two quirks differ from the default hash:
 * multiplication uses plain `*` (precision loss above 2^53), and the loop bound is `length / 4`
 * rather than `Math.floor(length / 4)` (out-of-range reads return 0).
 *
 * @see https://github.com/apache/kafka/blob/0.10.2/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L364
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

  // Remainder bytes: length%4 of 3 runs all three blocks, 2 the last two, 1 only the last.
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
