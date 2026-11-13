// CRC32-C (Castagnoli) — the polynomial RecordBatch v2 uses for its batch checksum, also used by
// iSCSI (RFC 3720).
//
// Prefer Node/OpenSSL CRC32C when the platform exposes it (`crypto.hash('crc32c', …)`). Stock
// Node.js builds and Bun (BoringSSL) don't, and no Bun API computes CRC32C (`Bun.hash.crc32` is
// the IEEE polynomial), so the JS slicing-by-16 implementation below is the usual path on both.

import crypto from 'node:crypto';

/** Reflected Castagnoli polynomial. */
const CASTAGNOLI_POLYNOMIAL = 0x82f63b78;

/**
 * Sixteen 256-entry lookup tables laid out back to back: entry `(k << 8) | byte` is the CRC of
 * `byte` followed by `k` zero bytes. Slicing-by-16 folds 16 input bytes per step with one lookup
 * each, which is several times faster than the classic byte-at-a-time loop (see `bench/crc32c.ts`).
 */
const CRC_C_TABLES = new Int32Array(16 * 256);
for (let byte = 0; byte < 256; byte++) {
  let crc = byte;
  for (let bit = 0; bit < 8; bit++) {
    crc = (crc & 1) === 1 ? (crc >>> 1) ^ CASTAGNOLI_POLYNOMIAL : crc >>> 1;
  }
  CRC_C_TABLES[byte] = crc;
}
for (let index = 256; index < CRC_C_TABLES.length; index++) {
  const previous = CRC_C_TABLES[index - 256] as number;
  CRC_C_TABLES[index] = (CRC_C_TABLES[previous & 0xff] as number) ^ (previous >>> 8);
}

/** ITU-T V.42 / Castagnoli check value for the ASCII string "123456789". */
const CRC32C_CHECK_INPUT = Buffer.from('123456789');
const CRC32C_CHECK_VALUE = 0xe3069283;

export function crc32cJs(buffer: Buffer): number {
  const table = CRC_C_TABLES;
  const length = buffer.length;
  const blocksEnd = length - (length % 16);
  let crc = -1;
  let i = 0;

  // Indexing bytes directly is faster on Node than `readInt32LE`, and equal on Bun.
  for (; i < blocksEnd; i += 16) {
    crc ^=
      (buffer[i] as number) |
      ((buffer[i + 1] as number) << 8) |
      ((buffer[i + 2] as number) << 16) |
      ((buffer[i + 3] as number) << 24);
    crc =
      (table[0xf00 | (crc & 0xff)] as number) ^
      (table[0xe00 | ((crc >>> 8) & 0xff)] as number) ^
      (table[0xd00 | ((crc >>> 16) & 0xff)] as number) ^
      (table[0xc00 | (crc >>> 24)] as number) ^
      (table[0xb00 | (buffer[i + 4] as number)] as number) ^
      (table[0xa00 | (buffer[i + 5] as number)] as number) ^
      (table[0x900 | (buffer[i + 6] as number)] as number) ^
      (table[0x800 | (buffer[i + 7] as number)] as number) ^
      (table[0x700 | (buffer[i + 8] as number)] as number) ^
      (table[0x600 | (buffer[i + 9] as number)] as number) ^
      (table[0x500 | (buffer[i + 10] as number)] as number) ^
      (table[0x400 | (buffer[i + 11] as number)] as number) ^
      (table[0x300 | (buffer[i + 12] as number)] as number) ^
      (table[0x200 | (buffer[i + 13] as number)] as number) ^
      (table[0x100 | (buffer[i + 14] as number)] as number) ^
      (table[buffer[i + 15] as number] as number);
  }

  for (; i < length; i++) {
    crc = (table[(crc ^ (buffer[i] as number)) & 0xff] as number) ^ (crc >>> 8);
  }

  return (crc ^ -1) >>> 0;
}

function resolveNativeCrc32c(): ((buffer: Buffer) => number) | undefined {
  try {
    if (typeof crypto.hash !== 'function') {
      return undefined;
    }

    const digest = crypto.hash('crc32c', CRC32C_CHECK_INPUT, 'buffer');
    if (digest.length !== 4) {
      return undefined;
    }

    if (digest.readUInt32BE(0) === CRC32C_CHECK_VALUE) {
      return (buffer: Buffer) => crypto.hash('crc32c', buffer, 'buffer').readUInt32BE(0);
    }

    if (digest.readUInt32LE(0) === CRC32C_CHECK_VALUE) {
      return (buffer: Buffer) => crypto.hash('crc32c', buffer, 'buffer').readUInt32LE(0);
    }

    return undefined;
  } catch {
    return undefined;
  }
}

const nativeCrc32c = resolveNativeCrc32c();

/** True when RecordBatch checksums use Node/OpenSSL CRC32C instead of the JS table. */
export const usesNativeCrc32c = nativeCrc32c !== undefined;

export function crc32c(buffer: Buffer): number {
  return nativeCrc32c !== undefined ? nativeCrc32c(buffer) : crc32cJs(buffer);
}
