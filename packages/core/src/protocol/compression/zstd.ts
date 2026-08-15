import { promisify } from 'node:util';
import { zstdCompress, zstdDecompress } from 'node:zlib';
import type { CompressionCodec } from './index.js';

const compress = promisify(zstdCompress);
const decompress = promisify(zstdDecompress);

/** Node 24 ships zstd natively (`node:zlib`), so unlike Snappy/LZ4 this needs no extra package. */
export const zstdCodec: CompressionCodec = {
  async compress(encoder) {
    return compress(encoder.buffer);
  },
  async decompress(buffer) {
    return decompress(buffer);
  },
};
