import { describe, expect, it, vi } from 'vitest';
import type { CommandSpec } from './args/define';
import type { Runtime } from './runtime';

const FIXTURE_COMMANDS: CommandSpec[] = [
  {
    path: ['topic', 'list'],
    summary: 'list topics',
    exitCodes: [0],
    run: vi.fn(async () => 0),
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

function fakeRuntime(argv: readonly string[]) {
  const stdoutWrite = vi.fn((_chunk: string) => true);
  const stderrWrite = vi.fn((_chunk: string) => true);
  const runtime = {
    argv,
    env: {},
    isTty: false,
    stdout: { write: stdoutWrite },
    stderr: { write: stderrWrite },
  } as unknown as Runtime;
  return { runtime, stdoutWrite, stderrWrite };
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
});
