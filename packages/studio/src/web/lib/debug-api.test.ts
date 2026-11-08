import { afterEach, describe, expect, it, vi } from 'vitest';
import { streamDebug, encodeText } from './debug-api';

vi.mock('./auth', () => ({ withAuthHeaders: (headers?: HeadersInit) => headers, rejectSession: () => {} }));
afterEach(() => vi.unstubAllGlobals());
function mockStream(frames: string[]) {
  const encoder = new TextEncoder();
  vi.stubGlobal('fetch', () =>
    Promise.resolve(
      new Response(
        new ReadableStream({
          start(controller) {
            for (const frame of frames) controller.enqueue(encoder.encode(frame));
            controller.close();
          },
        }),
        { status: 200 },
      ),
    ),
  );
}
describe('debugger operation transport', () => {
  it('parses frames split between network chunks and requires terminal completion', async () => {
    mockStream(['event: prog', 'ress\ndata: {"scanned":1}\n', '\nevent: result\ndata: {"complete":true}\n\n']);
    const report = vi.fn();
    await streamDebug('/api/debug/scan', {}, new AbortController().signal, report);
    expect(report.mock.calls).toEqual([
      ['progress', { scanned: 1 }],
      ['result', { complete: true }],
    ]);
  });
  it('reports an interrupted replay as uncertain instead of success', async () => {
    mockStream(['event: record\ndata: {"status":"sent"}\n\n']);
    await expect(streamDebug('/api/debug/replay', {}, new AbortController().signal, () => {})).rejects.toThrow(
      'acknowledged writes',
    );
  });
  it('encodes UTF-8 strings without corrupting non-ASCII text', () => {
    expect(encodeText('Привіт 👋')).toBe('0J/RgNC40LLRltGCIPCfkYs=');
  });
});
