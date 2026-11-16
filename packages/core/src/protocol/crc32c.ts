// CRC32-C (Castagnoli) — the polynomial RecordBatch v2 uses for its batch checksum, also used by
// iSCSI (RFC 3720).
//
// Prefer the optional `@node-rs/crc32` package (hardware CRC32C through N-API) and fall back to the
// JS slicing-by-16 implementation below when it isn't installed or its binary doesn't load. Stock
// Node.js and Bun builds don't support `crypto.hash('crc32c')`, and no Bun API computes CRC32C
// (`Bun.hash.crc32` is the IEEE polynomial).

import { createRequire } from 'node:module';

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

/** Which implementation computes record batch checksums: `@node-rs/crc32` or the built-in JS one. */
export type Crc32cImplementation = 'native' | 'javascript';

type Checksum = (buffer: Buffer) => number;

interface SelectedCrc32c {
  implementation: Crc32cImplementation;
  checksum: Checksum;
}

/**
 * Uses `@node-rs/crc32` when `requireModule` loads a working binding, otherwise the JS table.
 * `@node-rs/crc32` can itself fall back to `@node-rs/crc32-wasm32-wasi`, but only when that package
 * was installed separately; it is not one of its dependencies.
 */
export function selectCrc32c(requireModule: (id: string) => unknown): SelectedCrc32c {
  const javascript: SelectedCrc32c = { implementation: 'javascript', checksum: crc32cJs };
  try {
    const loaded = requireModule('@node-rs/crc32') as { crc32c?: unknown } | null;
    if (typeof loaded?.crc32c !== 'function') {
      return javascript;
    }

    const nativeCrc32c = loaded.crc32c as Checksum;
    // A binding that computes some other CRC would make every produced batch fail the broker's check.
    if (nativeCrc32c(CRC32C_CHECK_INPUT) !== CRC32C_CHECK_VALUE) {
      return javascript;
    }
    return { implementation: 'native', checksum: nativeCrc32c };
  } catch {
    return javascript;
  }
}

/**
 * Batches shorter than this use the JS table until a longer one loads `@node-rs/crc32`. Below it the
 * JS table takes at most about 1.4 µs per batch, about 1.2 µs more than native on Node.js 26 and
 * Bun 1.4 (macOS arm64), while loading the binding takes about 2 ms and adds 1 to 7.5 MiB of memory
 * (#206). A process that only sends small batches skips that load for at most 1.2 µs per batch.
 */
export const NATIVE_CRC32C_MIN_BYTES = 4 * 1024;

interface DeferredCrc32c {
  checksum: Checksum;
  implementation: () => Crc32cImplementation;
}

/**
 * Checksums with the JS table until a buffer of `NATIVE_CRC32C_MIN_BYTES` or more, or a call to
 * `implementation()`, makes it call `selectCrc32c(requireModule)`. From then on the selected
 * implementation checksums every buffer, and `requireModule` is never called again.
 */
export function createDeferredCrc32c(requireModule: (id: string) => unknown): DeferredCrc32c {
  let selected: SelectedCrc32c | undefined;

  function select(): SelectedCrc32c {
    selected ??= selectCrc32c(requireModule);
    return selected;
  }

  return {
    checksum(buffer) {
      if (selected !== undefined) {
        return selected.checksum(buffer);
      }
      if (buffer.length < NATIVE_CRC32C_MIN_BYTES) {
        return crc32cJs(buffer);
      }
      return select().checksum(buffer);
    },
    implementation: () => select().implementation,
  };
}

const processCrc32c = createDeferredCrc32c(createRequire(import.meta.url));

export function crc32c(buffer: Buffer): number {
  return processCrc32c.checksum(buffer);
}

/**
 * Reports which CRC32C implementation checksums record batches on produce and verifies them on
 * consume (`checkCrcs`).
 *
 * @returns `'native'` when the optional `@node-rs/crc32` package loaded, or `'javascript'` when
 * it is not installed or has no working binary for this platform. The first call loads the
 * package if no record batch of 4 KiB or more has done so yet; the result doesn't change afterwards.
 */
export function getCrc32cImplementation(): Crc32cImplementation {
  return processCrc32c.implementation();
}
