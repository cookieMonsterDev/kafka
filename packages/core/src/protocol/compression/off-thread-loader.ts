import type { CodecWorkerOp } from './codec-worker';

let offThreadModule: Promise<typeof import('./off-thread')> | undefined;

/**
 * `runCodecOp` from `./off-thread`, loaded on first Snappy/LZ4 use, so importing the client does
 * not load `node:worker_threads` and `node:os`.
 */
export async function runCodecOp(op: CodecWorkerOp, buffer: Buffer, maxOutputLength?: number): Promise<Buffer> {
  offThreadModule ??= import('./off-thread');
  const { runCodecOp: runOffThread } = await offThreadModule;
  return runOffThread(op, buffer, maxOutputLength);
}
