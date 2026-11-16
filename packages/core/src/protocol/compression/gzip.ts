import { KafkaNonRetriableError } from '../../errors';
import type { CompressionCodec } from './index';
import { MAX_DECOMPRESSED_SIZE } from './limits';

/** Loaded on first GZIP use, so importing the client loads neither `node:zlib` nor `node:util`. */
async function importGzip() {
  const [zlib, { promisify }] = await Promise.all([import('node:zlib'), import('node:util')]);
  return { gzip: promisify(zlib.gzip), unzip: promisify(zlib.unzip) };
}

let gzipFunctions: ReturnType<typeof importGzip> | undefined;

function loadGzip(): ReturnType<typeof importGzip> {
  gzipFunctions ??= importGzip();
  return gzipFunctions;
}

export async function decompressGzip(buffer: Buffer, maxOutputLength = MAX_DECOMPRESSED_SIZE): Promise<Buffer> {
  const { unzip } = await loadGzip();
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
    const { gzip } = await loadGzip();
    return level == null ? gzip(encoder.buffer) : gzip(encoder.buffer, { level });
  },
  async decompress(buffer) {
    return decompressGzip(buffer);
  },
};
