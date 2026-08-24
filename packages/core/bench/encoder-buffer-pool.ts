import { bufferPoolStats, clearBufferPool } from '../src/protocol/buffer-pool';
import { Encoder } from '../src/protocol/encoder';
import { createRequest } from '../src/protocol/request';
import { measure, printStats, type BenchStats } from './measure';

const REQUEST_COUNT = Number(process.env.BENCH_REQUESTS ?? 50_000);

/** A ~40-byte body, representative of a small RPC (Heartbeat, OffsetCommit, ...). */
function smallRequest() {
  return {
    apiKey: 0,
    apiVersion: 9,
    encode: async () =>
      new Encoder().writeString('bench-group').writeInt32(1).writeString('bench-member-0000000000000000000'),
  };
}

/**
 * `createRequest` allocates a throwaway request-body encoder per RPC (via `encode()`); once its
 * bytes are copied into the header-wrapping encoder it's released back to the pool. The
 * header-wrapping encoder itself is deliberately left unpooled — it's handed to
 * `socket.write()`, whose buffer must stay valid until the OS write completes, which the
 * connection layer doesn't currently track — so steady state should land near a 50% miss rate:
 * every body encoder is a pool hit, every header encoder is a fresh allocation.
 */
export async function benchEncoderBufferPool(): Promise<BenchStats> {
  clearBufferPool();

  const stats = await measure({
    name: `createRequest × ${REQUEST_COUNT} (buffer pool)`,
    warmup: 1,
    iterations: 1,
    messagesPerIter: REQUEST_COUNT,
    bytesPerIter: 0,
    run: async () => {
      for (let i = 0; i < REQUEST_COUNT; i++) {
        await createRequest({ correlationId: i, clientId: 'bench-client', request: smallRequest() });
      }
    },
  });
  printStats(stats);

  clearBufferPool(); // drop the warmup pass's stats, keep only the measured iteration's
  for (let i = 0; i < REQUEST_COUNT; i++) {
    await createRequest({ correlationId: i, clientId: 'bench-client', request: smallRequest() });
  }

  const { acquireCount, allocCount, releaseCount } = bufferPoolStats();
  const missRate = ((allocCount / acquireCount) * 100).toFixed(2);
  console.log(
    `  buffer pool (steady state, ${REQUEST_COUNT} requests): ${acquireCount} acquires, ` +
      `${allocCount} allocUnsafe misses (${missRate}%), ${releaseCount} releases`,
  );

  return stats;
}
