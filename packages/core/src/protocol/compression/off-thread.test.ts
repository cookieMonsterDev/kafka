import { execFile } from 'node:child_process';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { inspect } from 'node:util';
import { describe, expect, it } from 'vitest';
import { runCodecOp } from './off-thread';

const DRIVER = join(dirname(fileURLToPath(import.meta.url)), '../../../test/helpers/run-codec-ops.mjs');
const EXIT_TIMEOUT_MS = 10_000;

interface DriverRun {
  timedOut: boolean;
  code: number | null;
  stdout: string;
}

/**
 * Runs codec ops in a fresh process of the current runtime (`node` or `bun`) that never stops the
 * pool, and reports whether it exited on its own before {@link EXIT_TIMEOUT_MS}.
 */
function runDriver(scenario: string): Promise<DriverRun> {
  return new Promise((resolve) => {
    execFile(
      process.execPath,
      [DRIVER, scenario],
      { encoding: 'utf8', timeout: EXIT_TIMEOUT_MS, killSignal: 'SIGKILL' },
      (error, stdout) => {
        resolve({
          timedOut: error?.killed === true,
          code: error == null ? 0 : typeof error.code === 'number' ? error.code : null,
          stdout,
        });
      },
    );
  });
}

describe('protocol/compression/off-thread', () => {
  it('compresses and decompresses snappy off the calling tick', async () => {
    const input = Buffer.from('off-thread snappy');
    const pending = runCodecOp('snappy-compress', input);
    expect(inspect(pending)).toContain('pending');
    const compressed = await pending;
    const roundTrip = await runCodecOp('snappy-decompress', compressed);
    expect(roundTrip).toEqual(input);
  });

  it('compresses and decompresses lz4 off the calling tick', async () => {
    const input = Buffer.from('off-thread lz4');
    const pending = runCodecOp('lz4-compress', input);
    expect(inspect(pending)).toContain('pending');
    const compressed = await pending;
    const roundTrip = await runCodecOp('lz4-decompress', compressed);
    expect(roundTrip).toEqual(input);
  });

  it.each(['single', 'round-trip', 'concurrent', 'rejected'])(
    'lets the process exit while workers are idle (%s)',
    async (scenario) => {
      const run = await runDriver(scenario);

      expect(run.timedOut).toBe(false);
      expect(run.code).toBe(0);
      expect(JSON.parse(run.stdout)).toEqual({ ok: true, scenario });
    },
    EXIT_TIMEOUT_MS + 5_000,
  );
});
