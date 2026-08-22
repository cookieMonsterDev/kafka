/**
 * Self-contained Snappy/LZ4 JS implementation used both on the main thread (as a last-resort
 * fallback) and inside a `worker_threads` Worker. This file must not import other workspace
 * modules: Node loads it as a Worker entry (type-stripping only, no Vite), so only `node:*` and
 * package imports resolve.
 *
 * Kafka Snappy is xerial snappy-java framing; LZ4 is LZ4 Frame (LZ4F).
 */
import { isMainThread, parentPort } from 'node:worker_threads';
import { compress as lz4Compress, decompress as lz4Decompress, decompressBound } from 'lz4-lite';
import { compress as snappyCompress, uncompress as snappyUncompress } from 'snappyjs';

const XERIAL_MAGIC = Buffer.from([0x82, 0x53, 0x4e, 0x41, 0x50, 0x50, 0x59, 0x00]);
const XERIAL_HEADER_SIZE = 16;
const XERIAL_VERSION = 1;
const XERIAL_MIN_COMPATIBLE_VERSION = 1;
/** `org.xerial.snappy.SnappyOutputStream` default block size. */
const XERIAL_BLOCK_SIZE = 32 * 1024;

const DEFAULT_MAX_OUTPUT = 100 * 1024 * 1024;

export const codecWorkerUrl = import.meta.url;

export type CodecWorkerOp = 'snappy-compress' | 'snappy-decompress' | 'lz4-compress' | 'lz4-decompress';

export interface CodecWorkerRequest {
  id: number;
  op: CodecWorkerOp;
  buffer: Buffer | Uint8Array;
  maxOutputLength?: number;
}

export interface CodecWorkerResponse {
  id: number;
  ok: boolean;
  buffer?: Buffer;
  error?: { name: string; message: string };
}

function asBuffer(value: Buffer | Uint8Array): Buffer {
  return Buffer.isBuffer(value) ? value : Buffer.from(value);
}

function isXerialFramed(buffer: Buffer): boolean {
  return buffer.length >= XERIAL_HEADER_SIZE && buffer.subarray(0, XERIAL_MAGIC.length).equals(XERIAL_MAGIC);
}

function uncompressSnappyBlock(block: Buffer, maxLength: number): Buffer {
  try {
    return Buffer.from(snappyUncompress(block, maxLength) as Uint8Array);
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e);
    if (message.includes('too big')) {
      throw new Error(`Snappy decompressed output exceeds ${maxLength} bytes`, { cause: e });
    }
    throw e;
  }
}

export function compressSnappySync(input: Buffer): Buffer {
  const header = Buffer.allocUnsafe(XERIAL_HEADER_SIZE);
  XERIAL_MAGIC.copy(header, 0);
  header.writeInt32BE(XERIAL_VERSION, 8);
  header.writeInt32BE(XERIAL_MIN_COMPATIBLE_VERSION, 12);

  const parts: Buffer[] = [header];

  for (let offset = 0; offset < input.length; offset += XERIAL_BLOCK_SIZE) {
    const end = Math.min(offset + XERIAL_BLOCK_SIZE, input.length);
    const compressed = Buffer.from(snappyCompress(input.subarray(offset, end)) as Uint8Array);
    const chunk = Buffer.allocUnsafe(4 + compressed.length);
    chunk.writeUInt32BE(compressed.length, 0);
    compressed.copy(chunk, 4);
    parts.push(chunk);
  }

  return Buffer.concat(parts);
}

export function decompressSnappySync(input: Buffer, maxOutputLength = DEFAULT_MAX_OUTPUT): Buffer {
  if (!isXerialFramed(input)) {
    return uncompressSnappyBlock(input, maxOutputLength);
  }

  const chunks: Buffer[] = [];
  let offset = XERIAL_HEADER_SIZE;
  let total = 0;

  while (offset < input.length) {
    if (offset + 4 > input.length) {
      throw new Error('Invalid xerial Snappy framing: truncated chunk length');
    }

    const compressedLength = input.readUInt32BE(offset);
    offset += 4;

    if (offset + compressedLength > input.length) {
      throw new Error('Invalid xerial Snappy framing: truncated chunk');
    }

    const remaining = maxOutputLength - total;
    const uncompressed = uncompressSnappyBlock(input.subarray(offset, offset + compressedLength), remaining);
    total += uncompressed.length;
    chunks.push(uncompressed);
    offset += compressedLength;
  }

  return Buffer.concat(chunks, total);
}

export function compressLz4Sync(input: Buffer): Buffer {
  return Buffer.from(lz4Compress(input));
}

export function decompressLz4Sync(input: Buffer, maxOutputLength = DEFAULT_MAX_OUTPUT): Buffer {
  const bound = decompressBound(input);
  if (!Number.isFinite(bound) || bound > maxOutputLength) {
    throw new Error(`LZ4 decompressed output exceeds ${maxOutputLength} bytes`);
  }
  return Buffer.from(lz4Decompress(input, bound));
}

export function runCodecOpSync(op: CodecWorkerOp, buffer: Buffer, maxOutputLength?: number): Buffer {
  switch (op) {
    case 'snappy-compress':
      return compressSnappySync(buffer);
    case 'snappy-decompress':
      return decompressSnappySync(buffer, maxOutputLength ?? DEFAULT_MAX_OUTPUT);
    case 'lz4-compress':
      return compressLz4Sync(buffer);
    case 'lz4-decompress':
      return decompressLz4Sync(buffer, maxOutputLength ?? DEFAULT_MAX_OUTPUT);
  }
}

function onWorkerMessage(port: NonNullable<typeof parentPort>, msg: CodecWorkerRequest): void {
  try {
    const buffer = runCodecOpSync(msg.op, asBuffer(msg.buffer), msg.maxOutputLength);
    const response: CodecWorkerResponse = { id: msg.id, ok: true, buffer };
    port.postMessage(response);
  } catch (e) {
    const err = e instanceof Error ? e : new Error(String(e));
    const response: CodecWorkerResponse = {
      id: msg.id,
      ok: false,
      error: { name: err.name, message: err.message },
    };
    port.postMessage(response);
  }
}

if (!isMainThread) {
  const port = parentPort;
  if (port === null) {
    throw new Error('codec worker started without parentPort');
  }
  port.on('message', (msg: CodecWorkerRequest) => {
    onWorkerMessage(port, msg);
  });
}
