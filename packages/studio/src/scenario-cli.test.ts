import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { main } from './main';
import type { Runtime } from './runtime';

async function withFiles(run: (runtime: Runtime, directory: string) => Promise<void>) {
  const directory = await mkdtemp(join(tmpdir(), 'studio-scenario-cli-'));
  const runtime: Runtime = {
    cwd: directory,
    argv: ['--scenario', 'scenario.json'],
    env: { KAFKA_CONFIG: join(directory, 'kafka.config.mjs') },
    platform: 'linux',
    stdout: { write: () => true },
    stderr: { write: () => true },
    now: () => new Date(),
    signal: new AbortController().signal,
    exit: () => {
      throw new Error('Unexpected exit');
    },
  };
  try {
    await run(runtime, directory);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}
describe('headless scenario entry point', () => {
  it('refuses studio.readOnly from config before connecting a producer', async () => {
    await withFiles(async (runtime, directory) => {
      await writeFile(join(directory, 'kafka.config.mjs'), 'export default { studio: { readOnly: true } };');
      await writeFile(
        join(directory, 'scenario.json'),
        JSON.stringify({ version: 1, name: 'blocked', steps: [{ kind: 'produce', topic: 'orders', value: 'hello' }] }),
      );
      const errors: string[] = [];
      const code = await main({
        ...runtime,
        stderr: {
          write: (text) => {
            errors.push(text);
            return true;
          },
        },
      });
      expect(code).toBe(1);
      expect(errors.join('')).toContain('studio.readOnly');
    });
  });
  it('returns failure for an unsupported scenario version without starting a server', async () => {
    await withFiles(async (runtime, directory) => {
      await writeFile(join(directory, 'scenario.json'), '{"version":2}');
      expect(await main(runtime)).toBe(1);
    });
  });
});
