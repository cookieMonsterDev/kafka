import { compress, decompress, decompressBound } from 'lz4-lite';
import { KafkaNonRetriableError } from '../../errors';
import type { CompressionCodec } from './index';
import { MAX_DECOMPRESSED_SIZE } from './limits';

/**
 * Kafka record batches (magic 2) use the LZ4 Frame format (LZ4F), not raw blocks.
 * `lz4-lite` emits version-01 frames with independent blocks and a correct header
 * checksum — the same shape as Apache Kafka’s `KafkaLZ4BlockOutputStream`, not the
 * broken 0.8/0.9 descriptor.
 */
export async function decompressLz4(buffer: Buffer, maxOutputLength = MAX_DECOMPRESSED_SIZE): Promise<Buffer> {
  const bound = decompressBound(buffer);
  if (!Number.isFinite(bound) || bound > maxOutputLength) {
    throw new KafkaNonRetriableError(`LZ4 decompressed output exceeds ${maxOutputLength} bytes`);
  }
  return Buffer.from(decompress(buffer, bound));
}

export const lz4Codec: CompressionCodec = {
  async compress(encoder) {
    return Buffer.from(compress(encoder.buffer));
  },
  async decompress(buffer) {
    return decompressLz4(buffer);
  },
};
