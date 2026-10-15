import { describe, expect, it, vi } from 'vitest';
import { CliConfigError } from './errors/cli-config-error';
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
    stdin: { setEncoding: vi.fn(), on: vi.fn(), removeListener: vi.fn(), pause: vi.fn() },
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

  describe('with a broken config file', () => {
    const brokenConfig = vi.fn(async () => {
      throw new CliConfigError('Failed to load kafka config file "/work/kafka.config.mjs": boom');
    });

    it.each([[['complete', 'top']], [['completion', 'bash']]])('%j never reads the config file', async (argv) => {
      const loadConfig = vi.fn(brokenConfig);
      const runtime = createFakeRuntime({ argv, loadConfig });

      await expect(main(runtime)).resolves.toBe(0);
      expect(loadConfig).not.toHaveBeenCalled();
    });

    it('doctor reports the load error instead of failing', async () => {
      const stdoutWrite = vi.fn((_chunk: string) => true);
      const runtime = createFakeRuntime({ argv: ['doctor'], loadConfig: brokenConfig, stdout: { write: stdoutWrite } });

      await expect(main(runtime)).resolves.toBe(0);
      const written = stdoutWrite.mock.calls.map((call) => call[0]).join('');
      expect(written).toContain('config error      Failed to load kafka config file "/work/kafka.config.mjs": boom');
    });

    it('still fails a command that needs the config', async () => {
      const runtime = createFakeRuntime({ argv: ['profiles'], loadConfig: brokenConfig });

      await expect(main(runtime)).resolves.not.toBe(0);
    });
  });

  it('resolves to 130 on SIGINT while a command is still waiting on the broker', async () => {
    const controller = new AbortController();
    const runtime = createFakeRuntime({
      argv: ['topic', 'list'],
      signal: controller.signal,
      openAdmin: vi.fn(() => new Promise<never>(() => {})),
    });

    const result = main(runtime);
    await vi.waitFor(() => expect(runtime.openAdmin).toHaveBeenCalled());
    controller.abort('SIGINT');

    await expect(result).resolves.toBe(130);
  });
});
