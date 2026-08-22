import { KafkaNonRetriableError } from '../../errors';
import type { CompressionCodec } from './index';
import { MAX_DECOMPRESSED_SIZE } from './limits';
import { runCodecOp } from './off-thread';
import { resolveOptionalNativeLz4 } from './optional-native';

function rethrowCodecError(error: unknown, maxOutputLength: number): never {
  if (error instanceof KafkaNonRetriableError) throw error;
  const cause = error instanceof Error ? error : undefined;
  const message = cause?.message ?? String(error);
  if (message.includes('exceeds')) {
    throw new KafkaNonRetriableError(`LZ4 decompressed output exceeds ${maxOutputLength} bytes`, { cause });
  }
  if (cause) throw cause;
  throw new Error(message);
}

export async function decompressLz4(buffer: Buffer, maxOutputLength = MAX_DECOMPRESSED_SIZE): Promise<Buffer> {
  try {
    const native = await resolveOptionalNativeLz4();
    if (native) {
      const out = await native.decompress(buffer);
      if (out.length > maxOutputLength) {
        throw new KafkaNonRetriableError(`LZ4 decompressed output exceeds ${maxOutputLength} bytes`);
      }
      return out;
    }
    return await runCodecOp('lz4-decompress', buffer, maxOutputLength);
  } catch (e) {
    rethrowCodecError(e, maxOutputLength);
  }
}

async function compressLz4(buffer: Buffer): Promise<Buffer> {
  const native = await resolveOptionalNativeLz4();
  if (native) {
    return native.compress(buffer);
  }
  return runCodecOp('lz4-compress', buffer);
}

/**
 * Kafka record batches (magic 2) use the LZ4 Frame format (LZ4F), not raw blocks.
 * JS (`lz4-lite`) runs in a worker; an optional native `lz4` package is used only when it emits
 * version-01 frames with independent blocks — the same shape as Apache Kafka’s
 * `KafkaLZ4BlockOutputStream`.
 */
export const lz4Codec: CompressionCodec = {
  async compress(encoder) {
    return compressLz4(encoder.buffer);
  },
  async decompress(buffer) {
    return decompressLz4(buffer);
  },
};
