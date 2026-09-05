import { describe, expect, it, vi } from 'vitest';
import { main } from './main';
import type { Runtime } from './runtime';

function fakeRuntime(overrides: Partial<Runtime> = {}): {
  runtime: Runtime;
  stdout: (chunk: string) => boolean;
  stderr: (chunk: string) => boolean;
} {
  const stdout = vi.fn(() => true);
  const stderr = vi.fn(() => true);
  const runtime: Runtime = {
    argv: [],
    cwd: '/nonexistent-test-cwd',
    env: {},
    platform: 'linux',
    stdout: { write: stdout },
    stderr: { write: stderr },
    now: () => new Date(),
    exit: () => {
      throw new Error('exit() should not be called');
    },
    signal: new AbortController().signal,
    ...overrides,
  };
  return { runtime, stdout, stderr };
}

describe('main', () => {
  it('prints usage and exits 0 for --help without starting the server', async () => {
    const { runtime, stdout } = fakeRuntime({ argv: ['--help'] });
    const code = await main(runtime);
    expect(code).toBe(0);
    expect(stdout).toHaveBeenCalledWith(expect.stringContaining('Usage: kafka-studio'));
  });

  it('prints the version and exits 0 for --version without starting the server', async () => {
    const { runtime, stdout } = fakeRuntime({ argv: ['--version'] });
    const code = await main(runtime);
    expect(code).toBe(0);
    expect(stdout).toHaveBeenCalledWith(expect.stringMatching(/^\d+\.\d+\.\d+\n$/));
  });

  it('prints a usage error and exits 2 for an unknown flag', async () => {
    const { runtime, stderr } = fakeRuntime({ argv: ['--bogus'] });
    const code = await main(runtime);
    expect(code).toBe(2);
    expect(stderr).toHaveBeenCalledWith(expect.stringContaining('kafka-studio:'));
  });

  it('returns the signal exit code immediately when already aborted', async () => {
    const controller = new AbortController();
    controller.abort('SIGTERM');
    const { runtime } = fakeRuntime({ signal: controller.signal });
    await expect(main(runtime)).resolves.toBe(143);
  });

  it('starts the server, then shuts down cleanly on abort', async () => {
    const controller = new AbortController();
    const { runtime, stdout } = fakeRuntime({
      argv: ['--port', '59105', '--browser', 'none'],
      signal: controller.signal,
    });

    const run = main(runtime);
    // Give startStudio a tick to bind before signalling shutdown.
    await vi.waitFor(() => expect(stdout).toHaveBeenCalledWith(expect.stringContaining('kafka-studio listening')));
    controller.abort('SIGINT');

    await expect(run).resolves.toBe(130);
  });
});
