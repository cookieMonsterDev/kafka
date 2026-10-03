/**
 * Subprocess driver for the codec worker pool: runs codec ops without stopping the pool, so the
 * process only exits if idle workers leave the event loop free. Runtime-agnostic: run it with
 * `node` or `bun` (`process.execPath`).
 *
 * Usage: `<runtime> run-codec-ops.mjs <scenario>`, where scenario is one of:
 * - `single`: one snappy compress (the pool spawns more workers than it uses)
 * - `round-trip`: compress and decompress with every off-thread codec
 * - `concurrent`: more simultaneous jobs than pool workers, so jobs queue
 * - `rejected`: a decompress the worker rejects
 *
 * Prints one JSON line to stdout: `{ ok, scenario }` or `{ ok: false, message }`.
 */
import './register-src-resolve.mjs';

const { runCodecOp } = await import('../../src/protocol/compression/off-thread.ts');

const [, , scenario] = process.argv;

async function roundTrip(codec, input) {
  const compressed = await runCodecOp(`${codec}-compress`, input);
  const output = await runCodecOp(`${codec}-decompress`, compressed);
  if (!output.equals(input)) throw new Error(`${codec} round trip mismatch`);
}

const scenarios = {
  single: async () => {
    await runCodecOp('snappy-compress', Buffer.from('single'));
  },
  'round-trip': async () => {
    await roundTrip('snappy', Buffer.from('round-trip snappy'));
    await roundTrip('lz4', Buffer.from('round-trip lz4'));
  },
  concurrent: async () => {
    await Promise.all(
      Array.from({ length: 8 }, (_, i) => roundTrip(i % 2 === 0 ? 'snappy' : 'lz4', Buffer.from(`job ${i}`))),
    );
  },
  rejected: async () => {
    const rejected = await runCodecOp('lz4-decompress', Buffer.from('not lz4')).then(
      () => false,
      () => true,
    );
    if (!rejected) throw new Error('expected lz4-decompress of garbage to reject');
  },
};

const run = scenario == null ? undefined : scenarios[scenario];
if (run == null) {
  throw new Error(`Usage: run-codec-ops.mjs <${Object.keys(scenarios).join('|')}>`);
}

try {
  await run();
  console.log(JSON.stringify({ ok: true, scenario }));
} catch (error) {
  console.log(JSON.stringify({ ok: false, message: error instanceof Error ? error.message : String(error) }));
}
