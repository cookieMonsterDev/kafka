import { describe, expect, it, vi, beforeEach } from 'vitest';
import { compress as snappyCompress } from 'snappyjs';
import {
  compressLz4Sync,
  compressSnappySync,
  decompressLz4Sync,
  decompressSnappySync,
  runCodecOpSync,
} from './codec-worker';

const XERIAL_MAGIC = Buffer.from([0x82, 0x53, 0x4e, 0x41, 0x50, 0x50, 0x59, 0x00]);

describe('protocol/compression/codec-worker (sync helpers)', () => {
  it('round-trips snappy through the sync helpers', () => {
    const input = Buffer.from('hello sync snappy world');
    const compressed = compressSnappySync(input);
    expect(compressed.subarray(0, 8)).toEqual(XERIAL_MAGIC);
    expect(decompressSnappySync(compressed)).toEqual(input);
  });

  it('round-trips snappy payloads spanning multiple xerial blocks', () => {
    const input = Buffer.alloc(40 * 1024, 'z');
    const compressed = compressSnappySync(input);
    expect(decompressSnappySync(compressed)).toEqual(input);
  });

  it('decompresses a raw (unframed) snappy block', () => {
    const raw = Buffer.from('raw block, no xerial framing');
    const compressed = Buffer.from(snappyCompress(raw));
    expect(decompressSnappySync(compressed)).toEqual(raw);
  });

  it('throws when a xerial chunk length is truncated', () => {
    const truncated = Buffer.concat([XERIAL_MAGIC, Buffer.alloc(8), Buffer.from([0, 0])]);
    expect(() => decompressSnappySync(truncated)).toThrow('Invalid xerial Snappy framing: truncated chunk length');
  });

  it('throws when a xerial chunk body is truncated', () => {
    const header = Buffer.concat([XERIAL_MAGIC, Buffer.alloc(8)]);
    const lengthPrefix = Buffer.allocUnsafe(4);
    lengthPrefix.writeUInt32BE(100, 0);
    const truncated = Buffer.concat([header, lengthPrefix, Buffer.from('short')]);
    expect(() => decompressSnappySync(truncated)).toThrow('Invalid xerial Snappy framing: truncated chunk');
  });

  it('throws when the decompressed snappy output exceeds the max output length', () => {
    const input = Buffer.alloc(256, 'a');
    const compressed = Buffer.from(snappyCompress(input));
    expect(() => decompressSnappySync(compressed, 1)).toThrow(/exceeds 1 bytes/);
  });

  it('round-trips lz4 through the sync helpers', () => {
    const input = Buffer.from('hello sync lz4 world');
    const compressed = compressLz4Sync(input);
    expect(decompressLz4Sync(compressed)).toEqual(input);
  });

  it('throws when the decompressed lz4 output exceeds the max output length', () => {
    const input = Buffer.alloc(256, 'b');
    const compressed = compressLz4Sync(input);
    expect(() => decompressLz4Sync(compressed, 1)).toThrow(/exceeds 1 bytes/);
  });

  it('dispatches every op through runCodecOpSync', () => {
    const input = Buffer.from('dispatch me');

    const snappyCompressed = runCodecOpSync('snappy-compress', input);
    expect(runCodecOpSync('snappy-decompress', snappyCompressed)).toEqual(input);

    const lz4Compressed = runCodecOpSync('lz4-compress', input);
    expect(runCodecOpSync('lz4-decompress', lz4Compressed)).toEqual(input);
  });
});

describe('protocol/compression/codec-worker (worker entry point)', () => {
  beforeEach(() => {
    vi.resetModules();
    vi.doUnmock('node:worker_threads');
  });

  it('throws when started off the main thread without a parentPort', async () => {
    vi.doMock('node:worker_threads', () => ({ isMainThread: false, parentPort: null }));
    await expect(import('./codec-worker')).rejects.toThrow('codec worker started without parentPort');
  });

  it('replies with the compressed buffer for a successful op', async () => {
    const handlers = new Map<string, (msg: unknown) => void>();
    const fakePort = {
      on: vi.fn((event: string, handler: (msg: unknown) => void) => {
        handlers.set(event, handler);
      }),
      postMessage: vi.fn(),
    };
    vi.doMock('node:worker_threads', () => ({ isMainThread: false, parentPort: fakePort }));

    await import('./codec-worker');
    const messageHandler = handlers.get('message');
    expect(messageHandler).toBeTypeOf('function');

    const input = new Uint8Array(Buffer.from('hello worker thread'));
    messageHandler!({ id: 7, op: 'snappy-compress', buffer: input });

    expect(fakePort.postMessage).toHaveBeenCalledTimes(1);
    const response = fakePort.postMessage.mock.calls[0]![0] as {
      id: number;
      ok: boolean;
      buffer?: Buffer;
    };
    expect(response.id).toBe(7);
    expect(response.ok).toBe(true);
    expect(Buffer.isBuffer(response.buffer)).toBe(true);
  });

  it('replies with a structured error when the op fails', async () => {
    const handlers = new Map<string, (msg: unknown) => void>();
    const fakePort = {
      on: vi.fn((event: string, handler: (msg: unknown) => void) => {
        handlers.set(event, handler);
      }),
      postMessage: vi.fn(),
    };
    vi.doMock('node:worker_threads', () => ({ isMainThread: false, parentPort: fakePort }));

    await import('./codec-worker');
    const messageHandler = handlers.get('message');

    messageHandler!({ id: 9, op: 'lz4-decompress', buffer: Buffer.from('not an lz4 frame') });

    expect(fakePort.postMessage).toHaveBeenCalledTimes(1);
    const response = fakePort.postMessage.mock.calls[0]![0] as {
      id: number;
      ok: boolean;
      error?: { name: string; message: string };
    };
    expect(response.id).toBe(9);
    expect(response.ok).toBe(false);
    expect(response.error?.message).toBeTruthy();
  });
});
