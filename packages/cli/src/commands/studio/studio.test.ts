import { afterEach, describe, expect, it, vi } from 'vitest';
import type { CommandContext } from '../../args/define';
import { EMPTY_RESOLVED_CLI_CONFIG } from '../../testing/create-command-context';
import type { Runtime } from '../../runtime';

const STUDIO_SPECIFIER = '@cookiemonsterdev/kafka-studio';

function fakeContext(overrides: { flags?: Record<string, unknown>; signal?: AbortSignal }): {
  context: CommandContext;
  stdoutWrite: ReturnType<typeof vi.fn>;
  stderrWrite: ReturnType<typeof vi.fn>;
} {
  const stdoutWrite = vi.fn((_chunk: string) => true);
  const stderrWrite = vi.fn((_chunk: string) => true);

  const runtime = {
    argv: [],
    env: {},
    cwd: '/work',
    stdout: { write: stdoutWrite },
    stderr: { write: stderrWrite },
    stdin: { setEncoding: vi.fn(), on: vi.fn() },
    isTty: false,
    columns: 80,
    now: () => new Date(),
    exit: () => {
      throw new Error('exit() should not be called from a command');
    },
    openAdmin: vi.fn(),
    loadConfig: vi.fn(),
    signal: overrides.signal ?? new AbortController().signal,
  } as unknown as Runtime;

  const context: CommandContext = {
    runtime,
    flags: overrides.flags ?? {},
    positionals: [],
    output: {
      palette: {} as CommandContext['output']['palette'],
      log: {
        debug: vi.fn(),
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
      } as unknown as CommandContext['output']['log'],
      write: vi.fn(),
      error: vi.fn(),
      cliError: vi.fn(),
    },
    config: EMPTY_RESOLVED_CLI_CONFIG,
  };

  return { context, stdoutWrite, stderrWrite };
}

describe('studioCommand', () => {
  afterEach(() => {
    vi.doUnmock(STUDIO_SPECIFIER);
    vi.resetModules();
  });

  it('starts the studio server, forwards flags, and stops it once the runtime aborts', async () => {
    const stop = vi.fn(async () => {});
    const startStudio = vi.fn(async () => ({
      url: 'http://127.0.0.1:5757/',
      host: '127.0.0.1',
      port: 5757,
      token: 't',
      stop,
    }));
    vi.doMock(STUDIO_SPECIFIER, () => ({ startStudio }));
    vi.resetModules();

    const { studioCommand } = await import('./studio');
    const controller = new AbortController();
    const { context } = fakeContext({
      flags: { port: 5757, host: '0.0.0.0', browser: 'none', 'read-only': true },
      signal: controller.signal,
    });

    const run = studioCommand.run(context);
    controller.abort('SIGINT');
    const code = await run;

    expect(code).toBe(0);
    expect(startStudio).toHaveBeenCalledWith(
      { port: 5757, host: '0.0.0.0', browser: 'none', readOnly: true },
      expect.objectContaining({ cwd: '/work', argv: [] }),
    );
    expect(stop).toHaveBeenCalledTimes(1);
  });

  it('omits unset flags rather than passing them through as undefined', async () => {
    const startStudio = vi.fn(async () => ({
      url: 'http://127.0.0.1:5757/',
      host: '127.0.0.1',
      port: 5757,
      token: 't',
      stop: async () => {},
    }));
    vi.doMock(STUDIO_SPECIFIER, () => ({ startStudio }));
    vi.resetModules();

    const { studioCommand } = await import('./studio');
    const controller = new AbortController();
    const { context } = fakeContext({ signal: controller.signal });

    const run = studioCommand.run(context);
    controller.abort('SIGINT');
    await run;

    expect(startStudio).toHaveBeenCalledWith({ readOnly: false }, expect.anything());
  });

  it('prints an install hint and fails cleanly when the studio package is not installed', async () => {
    vi.doMock(STUDIO_SPECIFIER, () =>
      Promise.reject(Object.assign(new Error('Cannot find package'), { code: 'ERR_MODULE_NOT_FOUND' })),
    );
    vi.resetModules();

    const { studioCommand } = await import('./studio');
    const { context, stderrWrite } = fakeContext({});

    const code = await studioCommand.run(context);

    expect(code).toBe(1);
    expect(stderrWrite).toHaveBeenCalledWith(
      expect.stringContaining('npm install --global @cookiemonsterdev/kafka-studio'),
    );
  });

  it('rejects a non-integer --port before ever importing the studio package', async () => {
    const startStudio = vi.fn();
    vi.doMock(STUDIO_SPECIFIER, () => ({ startStudio }));
    vi.resetModules();

    const { studioCommand } = await import('./studio');
    const { context, stderrWrite } = fakeContext({ flags: { port: 5757.5 } });

    const code = await studioCommand.run(context);

    expect(code).toBe(2);
    expect(stderrWrite).toHaveBeenCalledWith(expect.stringContaining('--port must be an integer'));
    expect(startStudio).not.toHaveBeenCalled();
  });
});
