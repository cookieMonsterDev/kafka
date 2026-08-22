import { availableParallelism } from 'node:os';
import { fileURLToPath } from 'node:url';
import { Worker } from 'node:worker_threads';
import {
  codecWorkerUrl,
  runCodecOpSync,
  type CodecWorkerOp,
  type CodecWorkerRequest,
  type CodecWorkerResponse,
} from './codec-worker';

interface PendingJob {
  resolve: (buffer: Buffer) => void;
  reject: (error: Error) => void;
}

const POOL_SIZE = Math.max(1, Math.min(2, availableParallelism()));

let nextId = 1;
let workerFailed = false;
const workers: Worker[] = [];
const idle: Worker[] = [];
const pending = new Map<number, PendingJob>();
const queued: Array<{ request: CodecWorkerRequest; job: PendingJob }> = [];

function workerFilename(): string {
  const url = new URL(codecWorkerUrl);
  url.search = '';
  url.hash = '';
  return fileURLToPath(url);
}

function asError(error: { name: string; message: string } | undefined): Error {
  const err = new Error(error?.message ?? 'codec worker failed');
  if (error?.name) err.name = error.name;
  return err;
}

function asBuffer(value: Buffer | Uint8Array | undefined): Buffer {
  if (value == null) return Buffer.alloc(0);
  return Buffer.isBuffer(value) ? value : Buffer.from(value);
}

function dispatch(worker: Worker, request: CodecWorkerRequest): void {
  worker.ref();
  worker.postMessage(request);
}

function flushQueue(): void {
  while (queued.length > 0 && idle.length > 0) {
    const worker = idle.pop();
    const item = queued.shift();
    if (worker == null || item == null) break;
    pending.set(item.request.id, item.job);
    dispatch(worker, item.request);
  }
}

function failAll(error: Error): void {
  workerFailed = true;
  for (const job of pending.values()) {
    job.reject(error);
  }
  pending.clear();
  for (const item of queued.splice(0, queued.length)) {
    item.job.reject(error);
  }
}

function onWorkerMessage(worker: Worker, msg: CodecWorkerResponse): void {
  const job = pending.get(msg.id);
  pending.delete(msg.id);
  idle.push(worker);
  worker.unref();
  flushQueue();

  if (!job) return;
  if (!msg.ok) {
    job.reject(asError(msg.error));
    return;
  }
  job.resolve(asBuffer(msg.buffer));
}

function onWorkerError(error: Error): void {
  failAll(error);
}

function spawnWorker(): Worker {
  const worker = new Worker(workerFilename());
  worker.unref();
  worker.on('message', (msg: CodecWorkerResponse) => {
    onWorkerMessage(worker, msg);
  });
  worker.on('error', onWorkerError);
  worker.on('exit', (code) => {
    if (code !== 0 && pending.size > 0) {
      failAll(new Error(`codec worker exited with code ${code}`));
    }
  });
  return worker;
}

function ensurePool(): void {
  if (workers.length > 0 || workerFailed) return;
  try {
    for (let i = 0; i < POOL_SIZE; i++) {
      const worker = spawnWorker();
      workers.push(worker);
      idle.push(worker);
    }
  } catch (e) {
    workerFailed = true;
    const error = e instanceof Error ? e : new Error(String(e));
    failAll(error);
  }
}

function runOnWorker(op: CodecWorkerOp, buffer: Buffer, maxOutputLength?: number): Promise<Buffer> {
  ensurePool();
  if (workerFailed || workers.length === 0) {
    return Promise.reject(new Error('codec worker pool is unavailable'));
  }

  return new Promise((resolve, reject) => {
    const id = nextId++;
    const request: CodecWorkerRequest = { id, op, buffer, maxOutputLength };
    const job: PendingJob = { resolve, reject };
    const worker = idle.pop();
    if (worker) {
      pending.set(id, job);
      dispatch(worker, request);
      return;
    }
    queued.push({ request, job });
  });
}

/**
 * Compress/decompress off the event loop. Prefers a reused `worker_threads` pool; if the worker
 * cannot start (unusual in tests/bundlers), falls back to the next macrotask so callers still get
 * a Promise, at the cost of running JS codecs on the main thread.
 */
export async function runCodecOp(op: CodecWorkerOp, buffer: Buffer, maxOutputLength?: number): Promise<Buffer> {
  if (!workerFailed) {
    try {
      return await runOnWorker(op, buffer, maxOutputLength);
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      if (
        message.includes('codec worker') ||
        message.includes('Cannot find module') ||
        message.includes('ERR_MODULE')
      ) {
        workerFailed = true;
      } else {
        throw e;
      }
    }
  }

  return new Promise((resolve, reject) => {
    setImmediate(() => {
      try {
        resolve(runCodecOpSync(op, buffer, maxOutputLength));
      } catch (e) {
        reject(e instanceof Error ? e : new Error(String(e)));
      }
    });
  });
}

export async function stopCodecWorkers(): Promise<void> {
  const current = workers.splice(0, workers.length);
  idle.length = 0;
  await Promise.all(current.map((worker) => worker.terminate()));
}
