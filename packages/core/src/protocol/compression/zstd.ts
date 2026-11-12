import { promisify } from 'node:util';
import { constants as zlibConstants, zstdCompress, zstdDecompress } from 'node:zlib';
import { KafkaNonRetriableError } from '../../errors';
import { isBunNativeEnabled } from '../../runtime/runtime';
import type { CompressionCodec } from './index';
import { MAX_DECOMPRESSED_SIZE } from './limits';

const compress = promisify(zstdCompress);
const decompress = promisify(zstdDecompress);

/** Levels `Bun.zstdCompress` accepts; it throws for others that `node:zlib` takes (negative fast levels, clamping). */
const BUN_ZSTD_MIN_LEVEL = 1;
const BUN_ZSTD_MAX_LEVEL = 22;

/** The part of `globalThis.Bun` used here; `@types/bun` is not a dependency. */
interface BunZstd {
  zstdCompress(input: Uint8Array, options?: { level: number }): Promise<Buffer>;
}

/**
 * `Bun.zstdCompress` runs off the event loop and beats `node:zlib` on Bun (`bench/compression-zlib.ts`); its frames
 * decode with `node:zlib` and any zstd reader. Levels it rejects stay on `node:zlib`. Decompression stays on `node:zlib`
 * because Bun's decompress APIs take no output limit and cannot enforce `MAX_DECOMPRESSED_SIZE`.
 */
function bunZstdCompressorFor(level: number | undefined): BunZstd | undefined {
  const isBunLevel =
    level == null || (Number.isInteger(level) && level >= BUN_ZSTD_MIN_LEVEL && level <= BUN_ZSTD_MAX_LEVEL);
  return isBunLevel && isBunNativeEnabled() ? (globalThis as { Bun?: BunZstd }).Bun : undefined;
}

export async function decompressZstd(buffer: Buffer, maxOutputLength = MAX_DECOMPRESSED_SIZE): Promise<Buffer> {
  try {
    return await decompress(buffer, { maxOutputLength });
  } catch (e) {
    const error = e as NodeJS.ErrnoException;
    if (error.code === 'ERR_BUFFER_TOO_LARGE') {
      throw new KafkaNonRetriableError(`ZSTD decompressed output exceeds ${maxOutputLength} bytes`, {
        cause: error,
      });
    }
    throw error;
  }
}

/** Node ships zstd natively (`node:zlib`), so unlike Snappy/LZ4 this needs no extra package. */
export const zstdCodec: CompressionCodec = {
  async compress(encoder, level) {
    const bun = bunZstdCompressorFor(level);
    if (bun) return level == null ? bun.zstdCompress(encoder.buffer) : bun.zstdCompress(encoder.buffer, { level });
    if (level == null) return compress(encoder.buffer);
    return compress(encoder.buffer, { params: { [zlibConstants.ZSTD_c_compressionLevel]: level } });
  },
  async decompress(buffer) {
    return decompressZstd(buffer);
  },
};
