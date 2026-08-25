import { promisify } from 'node:util';
import { gzip as gzipCallback, unzip as unzipCallback } from 'node:zlib';
import { KafkaNonRetriableError } from '../../errors';
import type { CompressionCodec } from './index';
import { MAX_DECOMPRESSED_SIZE } from './limits';

const gzip = promisify(gzipCallback);
const unzip = promisify(unzipCallback);

export async function decompressGzip(buffer: Buffer, maxOutputLength = MAX_DECOMPRESSED_SIZE): Promise<Buffer> {
  try {
    return await unzip(buffer, { maxOutputLength });
  } catch (e) {
    const error = e as NodeJS.ErrnoException;
    if (error.code === 'ERR_BUFFER_TOO_LARGE') {
      throw new KafkaNonRetriableError(`GZIP decompressed output exceeds ${maxOutputLength} bytes`, {
        cause: error,
      });
    }
    throw error;
  }
}

export const gzipCodec: CompressionCodec = {
  async compress(encoder, level) {
    return level == null ? gzip(encoder.buffer) : gzip(encoder.buffer, { level });
  },
  async decompress(buffer) {
    return decompressGzip(buffer);
  },
};
