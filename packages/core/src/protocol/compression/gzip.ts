import { promisify } from 'node:util';
import { gzip as gzipCallback, unzip as unzipCallback } from 'node:zlib';
import type { CompressionCodec } from './index.js';

const gzip = promisify(gzipCallback);
const unzip = promisify(unzipCallback);

export const gzipCodec: CompressionCodec = {
  async compress(encoder) {
    return gzip(encoder.buffer);
  },
  async decompress(buffer) {
    return unzip(buffer);
  },
};
