import { compress as snappyCompress, uncompress as snappyUncompress } from 'snappyjs';
import { KafkaNonRetriableError } from '../../errors';
import type { CompressionCodec } from './index';
import { MAX_DECOMPRESSED_SIZE } from './limits';

/**
 * Kafka Snappy is xerial snappy-java framing, not a raw snappy block.
 * Magic `-126, 'S','N','A','P','P','Y', 0` then version / min-compatible int32 BE,
 * then repeating `[4-byte BE compressed length][snappy-compressed bytes]`.
 *
 * @see https://github.com/xerial/snappy-java
 */
const XERIAL_MAGIC = Buffer.from([0x82, 0x53, 0x4e, 0x41, 0x50, 0x50, 0x59, 0x00]);
const XERIAL_HEADER_SIZE = 16;
const XERIAL_VERSION = 1;
const XERIAL_MIN_COMPATIBLE_VERSION = 1;
/** `org.xerial.snappy.SnappyOutputStream` default block size. */
const XERIAL_BLOCK_SIZE = 32 * 1024;

function isXerialFramed(buffer: Buffer): boolean {
  return buffer.length >= XERIAL_HEADER_SIZE && buffer.subarray(0, XERIAL_MAGIC.length).equals(XERIAL_MAGIC);
}

function uncompressBlock(block: Buffer, maxLength: number): Buffer {
  try {
    return snappyUncompress(block, maxLength);
  } catch (e) {
    const error = e as Error;
    if (typeof error.message === 'string' && error.message.includes('too big')) {
      throw new KafkaNonRetriableError(`Snappy decompressed output exceeds ${maxLength} bytes`, {
        cause: error,
      });
    }
    throw error;
  }
}

export async function decompressSnappy(buffer: Buffer, maxOutputLength = MAX_DECOMPRESSED_SIZE): Promise<Buffer> {
  if (!isXerialFramed(buffer)) {
    return uncompressBlock(buffer, maxOutputLength);
  }

  const chunks: Buffer[] = [];
  let offset = XERIAL_HEADER_SIZE;
  let total = 0;

  while (offset < buffer.length) {
    if (offset + 4 > buffer.length) {
      throw new Error('Invalid xerial Snappy framing: truncated chunk length');
    }

    const compressedLength = buffer.readUInt32BE(offset);
    offset += 4;

    if (offset + compressedLength > buffer.length) {
      throw new Error('Invalid xerial Snappy framing: truncated chunk');
    }

    const remaining = maxOutputLength - total;
    const uncompressed = uncompressBlock(buffer.subarray(offset, offset + compressedLength), remaining);
    total += uncompressed.length;
    chunks.push(uncompressed);
    offset += compressedLength;
  }

  return Buffer.concat(chunks, total);
}

async function compressSnappy(buffer: Buffer): Promise<Buffer> {
  const header = Buffer.allocUnsafe(XERIAL_HEADER_SIZE);
  XERIAL_MAGIC.copy(header, 0);
  header.writeInt32BE(XERIAL_VERSION, 8);
  header.writeInt32BE(XERIAL_MIN_COMPATIBLE_VERSION, 12);

  const parts: Buffer[] = [header];

  for (let offset = 0; offset < buffer.length; offset += XERIAL_BLOCK_SIZE) {
    const end = Math.min(offset + XERIAL_BLOCK_SIZE, buffer.length);
    const compressed = snappyCompress(buffer.subarray(offset, end));
    const chunk = Buffer.allocUnsafe(4 + compressed.length);
    chunk.writeUInt32BE(compressed.length, 0);
    compressed.copy(chunk, 4);
    parts.push(chunk);
  }

  return Buffer.concat(parts);
}

/** Pure-JS Snappy (`snappyjs`) plus xerial framing so brokers can decompress. */
export const snappyCodec: CompressionCodec = {
  async compress(encoder) {
    return compressSnappy(encoder.buffer);
  },
  async decompress(buffer) {
    return decompressSnappy(buffer);
  },
};
