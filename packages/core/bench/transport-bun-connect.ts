/**
 * Socket transport: `node:net` (the default `SocketFactory`) against `Bun.connect`, both driven
 * through core's `Connection` (framing, correlation ids, request queue) against a local server
 * that answers Kafka-framed requests. The server runs in a child process on the same runtime, so
 * only the client's CPU is counted.
 *
 *   bun bench/transport-bun-connect.ts   # node:net and Bun.connect side by side
 *
 * Bun only: on Node.js there is nothing to compare, and the extensionless `src` imports need Vite, so
 * it is not registered in `bench/run.ts`.
 *
 * Workloads: `small` many in-flight round trips with tiny frames (per-request overhead),
 * `fetch` 1 MiB responses (read path), `produce` 1 MiB requests (write path, backpressure).
 * Each row is the median of `BENCH_ROUNDS` (default 5) runs; variants alternate within a round.
 * `cpu ms/1k` is client process CPU (user + system) per 1000 requests.
 *
 * Gate, fixed before measuring: keep a `Bun.connect` transport only if it gives at least 10%
 * more throughput or 10% less CPU per request on a workload, and the spikes show no unfixable
 * semantic gap. Result on Bun 1.4.0 and 1.4.4 (macOS arm64): no-go. `Bun.connect` was 5-7%
 * faster on `small`, within ±5% on `fetch` and 3-9% slower on `produce`; Bun's `node:net` already
 * beats Node.js here. So Bun keeps the `node:net` / `node:tls` path, which also honours every
 * `ssl` option (`Bun.connect` ignores `checkServerIdentity` and rejects `minVersion`).
 * Rerun this before reconsidering a native transport on a newer Bun.
 */
import { spawn } from 'node:child_process';
import { EventEmitter } from 'node:events';
import type { Socket } from 'node:net';
import { createInterface } from 'node:readline';
import { createLogger, LOG_LEVELS } from '../src/loggers/index';
import { Encoder } from '../src/protocol/encoder';
import { API_KEYS } from '../src/protocol/requests/api-keys';
import { Connection } from '../src/network/connection';
import { createDefaultSocketFactory, type SocketFactory } from '../src/network/socket-factory';

/** The subset of a Bun `Socket` this bench calls; `@types/bun` is not a dependency. */
interface BunSocket {
  write(data: Uint8Array): number;
  setNoDelay(noDelay: boolean): boolean;
  setKeepAlive(enable: boolean, initialDelayMs: number): boolean;
  terminate(): void;
  unref(): void;
}

interface BunConnect {
  connect(options: {
    hostname: string;
    port: number;
    socket: {
      open(socket: BunSocket): void;
      data(socket: BunSocket, data: Buffer): void;
      drain(socket: BunSocket): void;
      end(socket: BunSocket): void;
      close(socket: BunSocket, error?: Error): void;
      connectError(socket: BunSocket, error: Error): void;
    };
  }): Promise<BunSocket>;
}

interface Workload {
  name: string;
  requests: number;
  concurrency: number;
  requestBodyBytes: number;
  responseBodyBytes: number;
}

interface Sample {
  requestsPerSec: number;
  mbPerSec: number;
  cpuMsPer1k: number;
}

const ROUNDS = Number(process.env.BENCH_ROUNDS ?? 5);
const MIB = 1024 * 1024;
const SILENT_LOGGER = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

const WORKLOADS: Workload[] = [
  { name: 'small', requests: 100_000, concurrency: 16, requestBodyBytes: 64, responseBodyBytes: 100 },
  { name: 'fetch', requests: 2_000, concurrency: 4, requestBodyBytes: 64, responseBodyBytes: MIB },
  { name: 'produce', requests: 2_000, concurrency: 4, requestBodyBytes: MIB, responseBodyBytes: 16 },
];

/**
 * Reads `[int32 size][int16 apiKey][int16 apiVersion][int32 correlationId][string clientId][int32 responseBytes]...`
 * and answers `[int32 size][int32 correlationId][responseBytes]`.
 */
const SERVER_SOURCE = `
const net = require('node:net');
const bodies = new Map();
const server = net.createServer((socket) => {
  socket.setNoDelay(true);
  let buffered = Buffer.alloc(0);
  socket.on('data', (chunk) => {
    buffered = buffered.length === 0 ? chunk : Buffer.concat([buffered, chunk]);
    let offset = 0;
    while (buffered.length - offset >= 4) {
      const size = buffered.readInt32BE(offset);
      if (buffered.length - offset < 4 + size) break;
      const correlationId = buffered.readInt32BE(offset + 8);
      const clientIdLength = buffered.readInt16BE(offset + 12);
      const responseBytes = buffered.readInt32BE(offset + 14 + Math.max(clientIdLength, 0));
      let body = bodies.get(responseBytes);
      if (!body) { body = Buffer.alloc(responseBytes, 7); bodies.set(responseBytes, body); }
      const header = Buffer.allocUnsafe(8);
      header.writeInt32BE(4 + responseBytes, 0);
      header.writeInt32BE(correlationId, 4);
      socket.write(header);
      socket.write(body);
      offset += 4 + size;
    }
    buffered = buffered.subarray(offset);
  });
  socket.on('error', () => {});
});
server.listen(0, '127.0.0.1', () => console.log(server.address().port));
`;

function startServer(): Promise<{ port: number; stop: () => void }> {
  const child = spawn(process.execPath, ['-e', SERVER_SOURCE], { stdio: ['ignore', 'pipe', 'inherit'] });
  return new Promise((resolve, reject) => {
    child.once('error', reject);
    createInterface({ input: child.stdout }).once('line', (line) => {
      resolve({ port: Number(line), stop: () => child.kill() });
    });
  });
}

/**
 * A `net.Socket`-shaped emitter over `Bun.connect`, with the write queue Bun leaves to the caller:
 * `write` sends what the kernel accepts and queues the rest until `drain`.
 */
function bunConnectSocketFactory(bun: BunConnect): SocketFactory {
  return ({ host, port, onConnect }) => {
    const emitter = new EventEmitter();
    const pending: Buffer[] = [];
    let socket: BunSocket | undefined;

    const flush = (target: BunSocket): void => {
      while (pending.length > 0) {
        const head = pending[0]!;
        const written = target.write(head);
        if (written < head.length) {
          pending[0] = head.subarray(Math.max(written, 0));
          return;
        }
        pending.shift();
      }
    };

    const shim = Object.assign(emitter, {
      write(data: Buffer): boolean {
        if (!socket) throw new Error('write before connect');
        if (pending.length === 0) {
          const written = socket.write(data);
          if (written === data.length) return true;
          pending.push(data.subarray(Math.max(written, 0)));
          return false;
        }
        pending.push(data);
        return false;
      },
      destroy() {
        socket?.terminate();
        return shim;
      },
      unref() {
        socket?.unref();
        return shim;
      },
    });

    bun
      .connect({
        hostname: host,
        port,
        socket: {
          open(opened) {
            socket = opened;
            opened.setNoDelay(true);
            opened.setKeepAlive(true, 60_000);
            onConnect();
          },
          data: (_socket, data) => emitter.emit('data', data),
          drain: (drained) => flush(drained),
          end: () => emitter.emit('end'),
          close: (_socket, error) => {
            if (error) emitter.emit('error', error);
          },
          connectError: (_socket, error) => emitter.emit('error', error),
        },
      })
      .catch(() => {
        // Reported through `connectError`.
      });

    return shim as unknown as Socket;
  };
}

function benchRequest(workload: Workload) {
  const padding = Buffer.alloc(workload.requestBodyBytes - 4);
  return {
    apiKey: API_KEYS.Metadata,
    apiVersion: 0,
    apiName: 'Metadata',
    encode: () => Promise.resolve(new Encoder().writeInt32(workload.responseBodyBytes).writeBuffer(padding)),
  };
}

const passthroughResponse = {
  decode: async (rawData: Buffer) => rawData,
  parse: async (data: Buffer) => data,
};

async function runWorkload(socketFactory: SocketFactory, port: number, workload: Workload): Promise<Sample> {
  const connection = new Connection({
    host: '127.0.0.1',
    port,
    logger: SILENT_LOGGER,
    socketFactory,
    requestTimeout: 60_000,
    connectionTimeout: 5_000,
  });
  await connection.connect();

  const request = benchRequest(workload);
  let remaining = workload.requests;
  const worker = async (): Promise<void> => {
    while (remaining > 0) {
      remaining--;
      const payload = await connection.send({ request, response: passthroughResponse });
      if (payload?.length !== workload.responseBodyBytes) {
        throw new Error(`expected ${workload.responseBodyBytes} bytes, got ${payload?.length ?? 0}`);
      }
    }
  };

  const cpuBefore = process.cpuUsage();
  const started = performance.now();
  await Promise.all(Array.from({ length: workload.concurrency }, worker));
  const elapsedSec = (performance.now() - started) / 1000;
  const cpu = process.cpuUsage(cpuBefore);
  await connection.disconnect();

  const bytes = workload.requests * (workload.requestBodyBytes + workload.responseBodyBytes);
  return {
    requestsPerSec: workload.requests / elapsedSec,
    mbPerSec: bytes / MIB / elapsedSec,
    cpuMsPer1k: (cpu.user + cpu.system) / 1000 / (workload.requests / 1000),
  };
}

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)] ?? 0;
}

export async function benchTransportBunConnect(): Promise<void> {
  const bun = (globalThis as { Bun?: BunConnect & { version: string } }).Bun;
  const variants: { name: string; factory: SocketFactory }[] = [
    { name: 'node:net', factory: createDefaultSocketFactory() },
  ];
  if (bun) variants.push({ name: 'Bun.connect', factory: bunConnectSocketFactory(bun) });

  const runtime = bun ? `Bun ${bun.version}` : `Node.js ${process.version}`;
  console.log(`transport: ${runtime}, ${process.platform}/${process.arch}, median of ${ROUNDS} rounds`);

  const server = await startServer();
  try {
    for (const workload of WORKLOADS) {
      const samples = new Map<string, Sample[]>(variants.map((variant) => [variant.name, []]));
      await runWorkload(variants[0]!.factory, server.port, { ...workload, requests: workload.requests / 10 });
      for (let round = 0; round < ROUNDS; round++) {
        for (const variant of variants) {
          samples.get(variant.name)!.push(await runWorkload(variant.factory, server.port, workload));
        }
      }

      for (const variant of variants) {
        const runs = samples.get(variant.name)!;
        const row = [
          `${workload.name} (${workload.requests} req, ${workload.concurrency} in flight)`.padEnd(38),
          variant.name.padEnd(12),
          `${median(runs.map((run) => run.requestsPerSec)).toFixed(0)} req/s`.padStart(14),
          `${median(runs.map((run) => run.mbPerSec)).toFixed(1)} MB/s`.padStart(12),
          `${median(runs.map((run) => run.cpuMsPer1k)).toFixed(1)} cpu ms/1k`.padStart(18),
        ];
        console.log(row.join(' '));
      }
    }
  } finally {
    server.stop();
  }
}

const isDirect = process.argv[1] !== undefined && process.argv[1].includes('transport-bun-connect');
if (isDirect) {
  benchTransportBunConnect().catch((error: unknown) => {
    console.error(error);
    process.exitCode = 1;
  });
}
