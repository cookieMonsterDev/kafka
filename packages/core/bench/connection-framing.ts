import { EventEmitter } from 'node:events';
import type { Socket } from 'node:net';
import { createLogger, LOG_LEVELS } from '../src/loggers/index';
import { Encoder } from '../src/protocol/encoder';
import { API_KEYS } from '../src/protocol/requests/api-keys';
import { Connection } from '../src/network/connection';
import type { SocketFactory } from '../src/network/socket-factory';
import { measure, printStats, type BenchStats } from './measure';

const FOUR_MIB = 4 * 1024 * 1024;
const SILENT_LOGGER = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function splitChunks(buffer: Buffer, chunkSize: number): Buffer[] {
  const chunks: Buffer[] = [];
  for (let offset = 0; offset < buffer.length; offset += chunkSize) {
    chunks.push(buffer.subarray(offset, Math.min(offset + chunkSize, buffer.length)));
  }
  return chunks;
}

function buildFramedResponse(correlationId: number, bodySize: number): Buffer {
  const body = Buffer.alloc(bodySize, 7);
  const header = new Encoder().writeInt32(correlationId).writeBuffer(body);
  return new Encoder().writeInt32(header.size()).writeEncoder(header).buffer;
}

function chunkedSocketFactory(chunkSize: number, bodySize: number): SocketFactory {
  return ({ onConnect }) => {
    const socket = new EventEmitter() as EventEmitter & Socket;
    socket.end = (() => socket) as Socket['end'];
    socket.unref = (() => socket) as Socket['unref'];
    socket.write = ((data: Buffer | string) => {
      const request = Buffer.isBuffer(data) ? data : Buffer.from(data, 'binary');
      const correlationId = request.readInt32BE(8);
      const framed = buildFramedResponse(correlationId, bodySize);
      const chunks = splitChunks(framed, chunkSize);
      queueMicrotask(() => {
        for (const chunk of chunks) {
          socket.emit('data', chunk);
        }
      });
      return true;
    }) as Socket['write'];

    queueMicrotask(onConnect);
    return socket;
  };
}

const metadataRequest = () => ({
  apiKey: API_KEYS.Metadata,
  apiVersion: 0,
  apiName: 'Metadata',
  encode: () => Promise.resolve(new Encoder()),
});

const passthroughResponse = {
  decode: async (rawData: Buffer) => rawData,
  parse: async (data: Buffer) => data,
};

async function timeFraming(chunkSize: number, bodySize: number, iterations: number): Promise<BenchStats> {
  return measure({
    name: `connection framing ${bodySize / 1024} KiB body, ${chunkSize} byte chunks`,
    warmup: 1,
    iterations,
    messagesPerIter: 1,
    bytesPerIter: bodySize,
    run: async () => {
      const connection = new Connection({
        host: '127.0.0.1',
        port: 9092,
        logger: SILENT_LOGGER,
        socketFactory: chunkedSocketFactory(chunkSize, bodySize),
        requestTimeout: 60_000,
        connectionTimeout: 1000,
      });
      await connection.connect();
      const payload = await connection.send({ request: metadataRequest(), response: passthroughResponse });
      if (!payload || payload.length !== bodySize) {
        throw new Error(`expected ${bodySize} byte body, got ${payload?.length ?? 0}`);
      }
      await connection.disconnect();
    },
  });
}

export async function benchConnectionFraming(): Promise<BenchStats> {
  const kib64 = 64 * 1024;

  const large64k = await timeFraming(kib64, FOUR_MIB, 5);
  printStats(large64k);

  const small1b = await timeFraming(1, kib64, 3);
  printStats(small1b);

  if (process.env.BENCH_FRAMING_1_BYTE === '1') {
    console.log('BENCH_FRAMING_1_BYTE=1: 4 MiB body in 1-byte chunks (quadratic before W4)');
    const large1b = await timeFraming(1, FOUR_MIB, 1);
    printStats(large1b);
    return large1b;
  }

  console.log(
    'skipping 4 MiB × 1-byte chunks (set BENCH_FRAMING_1_BYTE=1 to run; O(n²) concat is pathological before W4)',
  );
  return large64k;
}
