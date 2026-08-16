// Based on the kafka client 0.10.2 murmur2 implementation
// https://github.com/apache/kafka/blob/0.10.2/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L364

// 'm' and 'r' are mixing constants generated offline. They're not really 'magic', they just
// happen to work well.
const SEED = 0x9747b28c | 0;
const M = 0x5bd1e995 | 0;
const R = 24;

/**
 * Java-compatible murmur2. `Math.imul` gives the same 32-bit wraparound as Java `int` multiply.
 *
 * @see https://github.com/apache/kafka/blob/0.10.2/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L364
 */
export function murmur2(key: Buffer | string | number): number {
  const data = Buffer.isBuffer(key) ? key : Buffer.from(String(key));
  const length = data.length;

  let h = (SEED ^ length) | 0;
  const length4 = Math.floor(length / 4);

  for (let i = 0; i < length4; i++) {
    const i4 = i * 4;
    let k =
      data.readUInt8(i4) +
      (data.readUInt8(i4 + 1) << 8) +
      (data.readUInt8(i4 + 2) << 16) +
      (data.readUInt8(i4 + 3) << 24);

    k = Math.imul(k, M);
    k ^= k >>> R;
    k = Math.imul(k, M);
    h = Math.imul(h, M);
    h ^= k;
  }

  // Remainder bytes: length%4 of 3 runs all three blocks, 2 the last two, 1 only the last.
  const remainder = length % 4;
  if (remainder >= 3) {
    h ^= data.readUInt8((length & ~3) + 2) << 16;
  }
  if (remainder >= 2) {
    h ^= data.readUInt8((length & ~3) + 1) << 8;
  }
  if (remainder >= 1) {
    h ^= data.readUInt8(length & ~3);
    h = Math.imul(h, M);
  }

  h ^= h >>> 13;
  h = Math.imul(h, M);
  h ^= h >>> 15;

  return h | 0;
}
