import { describe, expect, it, vi } from 'vitest';
import type { CommandSpec } from './args/define';
import type { Runtime } from './runtime';
import { EMPTY_RESOLVED_CLI_CONFIG } from './testing/create-command-context';

const echoConfigRun = vi.fn(async () => 0);
let capturedFormat: 'human' | 'json' | undefined;

const FIXTURE_COMMANDS: CommandSpec[] = [
  {
    path: ['topic', 'list'],
    summary: 'list topics',
    exitCodes: [0],
    run: vi.fn(async () => 0),
  },
  {
    path: ['echo'],
    summary: 'echoes the config it received, for dispatch-level assertions',
    exitCodes: [0],
    run: echoConfigRun,
  },
  {
    path: ['probe-format'],
    summary: 'reports which renderer its output port actually called, for format-precedence tests',
    exitCodes: [0],
    run: async ({ output }) => {
      output.write({
        human: () => {
          capturedFormat = 'human';
          return 'human';
        },
        json: () => {
          capturedFormat = 'json';
          return '{}';
        },
      });
      return 0;
    },
  },
  {
    path: ['topic', 'create'],
    summary: 'create a topic',
    flags: [{ name: 'partitions', type: 'number', brief: 'partition count' }],
    positionals: [{ name: 'topic', brief: 'topic name' }],
    exitCodes: [0, 2],
    run: vi.fn(async ({ flags, positionals }) => {
      if (positionals[0] === undefined) return 2;
      return typeof flags.partitions === 'number' ? 0 : 1;
    }),
  },
];

vi.mock('./commands/index', () => ({ ALL_COMMANDS: FIXTURE_COMMANDS }));

const { dispatch } = await import('./dispatch');

function fakeRuntime(
  argv: readonly string[],
  overrides: { loadConfig?: Runtime['loadConfig']; env?: Record<string, string | undefined> } = {},
) {
  const stdoutWrite = vi.fn((_chunk: string) => true);
  const stderrWrite = vi.fn((_chunk: string) => true);
  const loadConfig = vi.fn(overrides.loadConfig ?? (async () => EMPTY_RESOLVED_CLI_CONFIG));
  const runtime = {
    argv,
    env: overrides.env ?? {},
    cwd: '/work',
    isTty: false,
    stdout: { write: stdoutWrite },
    stderr: { write: stderrWrite },
    loadConfig,
  } as unknown as Runtime;
  return { runtime, stdoutWrite, stderrWrite, loadConfig };
}

describe('dispatch', () => {
  it('shows root help and exits 0 for an empty argv', async () => {
    const { runtime, stdoutWrite } = fakeRuntime([]);
    await expect(dispatch(runtime)).resolves.toBe(0);
    expect(stdoutWrite).toHaveBeenCalledWith(expect.stringContaining('Usage: kafka'));
  });

  it('shows the version and exits 0 for "version"', async () => {
    const { runtime, stdoutWrite } = fakeRuntime(['version']);
    await expect(dispatch(runtime)).resolves.toBe(0);
    expect(stdoutWrite).toHaveBeenCalledWith(expect.stringMatching(/^\d+\.\d+\.\d+/));
  });

  it('shows the version and exits 0 for "--version"', async () => {
    const { runtime } = fakeRuntime(['--version']);
    await expect(dispatch(runtime)).resolves.toBe(0);
  });

  it('shows leaf help for "help topic list"', async () => {
    const { runtime, stdoutWrite } = fakeRuntime(['help', 'topic', 'list']);
    await expect(dispatch(runtime)).resolves.toBe(0);
    expect(stdoutWrite).toHaveBeenCalledWith(expect.stringContaining('list topics'));
  });

  it('shows leaf help for "topic list --help" instead of running it', async () => {
    const { runtime } = fakeRuntime(['topic', 'list', '--help']);
    await expect(dispatch(runtime)).resolves.toBe(0);
    const listCommand = FIXTURE_COMMANDS[0];
    expect(listCommand?.run).not.toHaveBeenCalled();
  });

  it('runs the matched command and returns its exit code', async () => {
    const { runtime } = fakeRuntime(['topic', 'create', 'orders', '--partitions', '3']);
    await expect(dispatch(runtime)).resolves.toBe(0);
  });

  it('maps a CliUsageError from argument parsing to the usage exit code', async () => {
    const { runtime, stderrWrite } = fakeRuntime(['topic', 'create', 'orders', '--partitions', 'nope']);
    await expect(dispatch(runtime)).resolves.toBe(2);
    expect(stderrWrite).toHaveBeenCalledWith(expect.stringContaining('--partitions'));
  });

  it('shows group help for a bare group path', async () => {
    const { runtime, stdoutWrite } = fakeRuntime(['topic']);
    await expect(dispatch(runtime)).resolves.toBe(0);
    expect(stdoutWrite).toHaveBeenCalledWith(expect.stringContaining('topic list'));
  });

  it('reports an unknown command as a usage error', async () => {
    const { runtime, stderrWrite } = fakeRuntime(['nonexistent']);
    await expect(dispatch(runtime)).resolves.toBe(2);
    expect(stderrWrite).toHaveBeenCalledWith(expect.stringContaining('unknown command'));
  });

  it('maps a malformed global flag to the usage exit code instead of throwing', async () => {
    // Regression: a bad --format value used to throw out of extractGlobalFlags before an output
    // port existed to report it, crashing the process instead of resolving to an exit code.
    const { runtime, stderrWrite } = fakeRuntime(['topic', 'list', '--format', 'yaml']);
    await expect(dispatch(runtime)).resolves.toBe(2);
    expect(stderrWrite).toHaveBeenCalledWith(expect.stringContaining('--format'));
  });

  describe('config resolution', () => {
    it('never calls loadConfig for --help or --version', async () => {
      const { runtime, loadConfig } = fakeRuntime(['--version']);
      await dispatch(runtime);
      expect(loadConfig).not.toHaveBeenCalled();

      const { runtime: helpRuntime, loadConfig: helpLoadConfig } = fakeRuntime(['topic', 'list', '--help']);
      await dispatch(helpRuntime);
      expect(helpLoadConfig).not.toHaveBeenCalled();
    });

    it('forwards --config-file and --profile to loadConfig', async () => {
      const { runtime, loadConfig } = fakeRuntime(['echo', '--config-file', './my.config.ts', '--profile', 'staging']);

      await dispatch(runtime);

      expect(loadConfig).toHaveBeenCalledWith(
        expect.objectContaining({ configFlag: './my.config.ts', profileFlag: 'staging' }),
      );
    });

    it("passes the resolved config through to the command's context", async () => {
      const resolved = { ...EMPTY_RESOLVED_CLI_CONFIG, profile: 'staging' };
      const { runtime } = fakeRuntime(['echo'], { loadConfig: async () => resolved });

      await dispatch(runtime);

      expect(echoConfigRun).toHaveBeenCalledWith(expect.objectContaining({ config: resolved }));
    });

    it('a loadConfig failure (e.g. an unknown --profile) maps to the config exit code', async () => {
      class FakeCliConfigError extends Error {
        override name = 'CliConfigError';
      }
      const { runtime, stderrWrite } = fakeRuntime(['echo', '--profile', 'bogus'], {
        loadConfig: async () => {
          throw new FakeCliConfigError('unknown profile "bogus"');
        },
      });

      await expect(dispatch(runtime)).resolves.toBe(3);
      expect(stderrWrite).toHaveBeenCalledWith(expect.stringContaining('unknown profile'));
    });

    it('applies cli.output as the lowest-precedence output format', async () => {
      capturedFormat = undefined;
      const resolved = { ...EMPTY_RESOLVED_CLI_CONFIG, cli: { output: 'json' as const } };
      const { runtime } = fakeRuntime(['probe-format'], { loadConfig: async () => resolved });

      await dispatch(runtime);

      expect(capturedFormat).toBe('json');
    });

    it('an explicit --format beats cli.output', async () => {
      capturedFormat = undefined;
      const resolved = { ...EMPTY_RESOLVED_CLI_CONFIG, cli: { output: 'json' as const } };
      const { runtime } = fakeRuntime(['probe-format', '--format', 'human'], { loadConfig: async () => resolved });

      await dispatch(runtime);

      expect(capturedFormat).toBe('human');
    });

    it('KAFKA_OUTPUT beats cli.output', async () => {
      capturedFormat = undefined;
      const resolved = { ...EMPTY_RESOLVED_CLI_CONFIG, cli: { output: 'json' as const } };
      const { runtime } = fakeRuntime(['probe-format'], {
        loadConfig: async () => resolved,
        env: { KAFKA_OUTPUT: 'human' },
      });

      await dispatch(runtime);

      expect(capturedFormat).toBe('human');
    });
  });
});
