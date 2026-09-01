import { describe, expect, it, vi } from 'vitest';
import { main } from './main';
import type { Runtime } from './runtime';
import { EMPTY_RESOLVED_CLI_CONFIG } from './testing/create-command-context';

function createFakeRuntime(overrides: Partial<Runtime> = {}): Runtime {
  const controller = new AbortController();
  return {
    argv: [],
    env: {},
    cwd: '/work',
    stdout: { write: vi.fn(() => true) },
    stderr: { write: vi.fn(() => true) },
    stdin: { setEncoding: vi.fn(), on: vi.fn() },
    isTty: false,
    columns: 80,
    now: () => new Date('2026-01-01T00:00:00Z'),
    exit: vi.fn(() => {
      throw new Error('exit() should never be called by main()');
    }),
    openAdmin: vi.fn(),
    loadConfig: vi.fn(async () => EMPTY_RESOLVED_CLI_CONFIG),
    signal: controller.signal,
    ...overrides,
  };
}

describe('main', () => {
  it('resolves to an exit code rather than throwing or calling exit()', async () => {
    const exitSpy = vi.fn(() => {
      throw new Error('exit() should never be called by main()');
    });
    const runtime = createFakeRuntime({ exit: exitSpy });
    const code = await main(runtime);

    expect(typeof code).toBe('number');
    expect(exitSpy).not.toHaveBeenCalled();
  });

  it('resolves to 130 when the signal is already aborted with reason SIGINT', async () => {
    const controller = new AbortController();
    controller.abort('SIGINT');
    const runtime = createFakeRuntime({ signal: controller.signal });

    await expect(main(runtime)).resolves.toBe(130);
  });

  it('resolves to 143 when the signal is already aborted with reason SIGTERM', async () => {
    const controller = new AbortController();
    controller.abort('SIGTERM');
    const runtime = createFakeRuntime({ signal: controller.signal });

    await expect(main(runtime)).resolves.toBe(143);
  });
});
