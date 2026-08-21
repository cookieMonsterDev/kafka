import { promisify } from 'node:util';
import { zstdCompress, zstdDecompress } from 'node:zlib';
import { KafkaNonRetriableError } from '../../errors';
import type { CompressionCodec } from './index';
import { MAX_DECOMPRESSED_SIZE } from './limits';

const compress = promisify(zstdCompress);
const decompress = promisify(zstdDecompress);

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

/** Node 24 ships zstd natively (`node:zlib`), so unlike Snappy/LZ4 this needs no extra package. */
export const zstdCodec: CompressionCodec = {
  async compress(encoder) {
    return compress(encoder.buffer);
  },
  async decompress(buffer) {
    return decompressZstd(buffer);
  },
};
