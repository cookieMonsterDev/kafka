import { createRequire } from 'node:module';

const optionalRequire = createRequire(import.meta.url);

export interface OptionalNativeCodec {
  compress(buffer: Buffer): Promise<Buffer>;
  decompress(buffer: Buffer, maxOutputLength?: number): Promise<Buffer>;
}

function isThenable(value: unknown): value is Promise<unknown> {
  return typeof value === 'object' && value !== null && 'then' in value && typeof value.then === 'function';
}

function asBuffer(value: unknown): Buffer {
  if (Buffer.isBuffer(value)) return value;
  if (value instanceof Uint8Array) return Buffer.from(value);
  throw new TypeError('Optional native codec did not return a Buffer');
}

function tryOptionalModule(id: string): Record<string, unknown> | null {
  try {
    const loaded: unknown = optionalRequire(id);
    if (typeof loaded === 'object' && loaded !== null) {
      return loaded as Record<string, unknown>;
    }
    return null;
  } catch {
    return null;
  }
}

function pickFn(mod: Record<string, unknown>, names: readonly string[]): ((input: Buffer) => unknown) | null {
  for (const name of names) {
    const value = mod[name];
    if (typeof value === 'function') {
      return value as (input: Buffer) => unknown;
    }
  }
  return null;
}

function wrapAsync(
  compressFn: (buffer: Buffer) => unknown,
  decompressFn: (buffer: Buffer) => unknown,
): OptionalNativeCodec {
  return {
    async compress(buffer) {
      return asBuffer(await compressFn(buffer));
    },
    async decompress(buffer) {
      return asBuffer(await decompressFn(buffer));
    },
  };
}

/**
 * Optional `snappy` package (native). Only accepted when `compress` is async so the event loop is
 * not blocked. Used for raw snappy blocks; xerial framing stays in JS on the caller.
 */
export function loadOptionalNativeSnappy(): OptionalNativeCodec | null {
  const mod = tryOptionalModule('snappy');
  if (!mod) return null;

  const compressFn = pickFn(mod, ['compress']);
  const decompressFn = pickFn(mod, ['uncompress', 'decompress']);
  if (!compressFn || !decompressFn) return null;

  try {
    const probe = compressFn(Buffer.from('k'));
    if (!isThenable(probe)) return null;
  } catch {
    return null;
  }

  return wrapAsync(compressFn, decompressFn);
}

const LZ4_FRAME_MAGIC0 = 0x04;
const LZ4_FRAME_MAGIC1 = 0x22;
const LZ4_FRAME_MAGIC2 = 0x4d;
const LZ4_FRAME_MAGIC3 = 0x18;

function isLz4Frame(buffer: Buffer): boolean {
  return (
    buffer.length >= 4 &&
    buffer[0] === LZ4_FRAME_MAGIC0 &&
    buffer[1] === LZ4_FRAME_MAGIC1 &&
    buffer[2] === LZ4_FRAME_MAGIC2 &&
    buffer[3] === LZ4_FRAME_MAGIC3
  );
}

/**
 * Optional `lz4` package (native). Accepted only when encode is async and the output is LZ4 Frame
 * (magic 0x184D2204), matching Kafka magic-2 record batches.
 */
export async function loadOptionalNativeLz4(): Promise<OptionalNativeCodec | null> {
  const mod = tryOptionalModule('lz4');
  if (!mod) return null;

  const compressFn = pickFn(mod, ['encode', 'compress']);
  const decompressFn = pickFn(mod, ['decode', 'decompress']);
  if (!compressFn || !decompressFn) return null;

  try {
    const probe = compressFn(Buffer.from('kafka-lz4-probe'));
    if (!isThenable(probe)) return null;
    const encoded = asBuffer(await probe);
    if (!isLz4Frame(encoded)) return null;
    return wrapAsync(compressFn, decompressFn);
  } catch {
    return null;
  }
}

let snappyCache: OptionalNativeCodec | null | undefined;
let lz4Cache: OptionalNativeCodec | null | undefined;
let lz4Pending: Promise<OptionalNativeCodec | null> | null = null;

export function resolveOptionalNativeSnappy(): OptionalNativeCodec | null {
  if (snappyCache !== undefined) return snappyCache;
  snappyCache = loadOptionalNativeSnappy();
  return snappyCache;
}

export async function resolveOptionalNativeLz4(): Promise<OptionalNativeCodec | null> {
  if (lz4Cache !== undefined) return lz4Cache;
  if (lz4Pending) return lz4Pending;

  lz4Pending = loadOptionalNativeLz4().then((codec) => {
    lz4Cache = codec;
    lz4Pending = null;
    return codec;
  });
  return lz4Pending;
}
