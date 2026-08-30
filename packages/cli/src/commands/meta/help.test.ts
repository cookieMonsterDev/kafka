import { describe, expect, it, vi } from 'vitest';
import type { CommandSpec } from '../../args/define';
import { commandGroups, createRegistry } from '../../registry';
import { runHelpCommand } from './help';

const OPTIONS = { programName: 'kafka' };

const FIXTURE_COMMANDS: CommandSpec[] = [
  { path: ['ping'], summary: 'ping', exitCodes: [0], run: async () => 0 },
  { path: ['topic', 'list'], summary: 'list topics', exitCodes: [0], run: async () => 0 },
];

function fakeRuntime() {
  const stdoutWrite = vi.fn((_chunk: string) => true);
  const stderrWrite = vi.fn((_chunk: string) => true);
  const runtime = {
    stdout: { write: stdoutWrite },
    stderr: { write: stderrWrite },
  } as unknown as Parameters<typeof runHelpCommand>[0];
  return { runtime, stdoutWrite, stderrWrite };
}

describe('runHelpCommand', () => {
  it('writes root help and returns 0 for an empty path', () => {
    const registry = createRegistry(FIXTURE_COMMANDS);
    const groups = commandGroups(registry);
    const { runtime, stdoutWrite } = fakeRuntime();

    const code = runHelpCommand(runtime, FIXTURE_COMMANDS, groups, [], OPTIONS);

    expect(code).toBe(0);
    expect(stdoutWrite).toHaveBeenCalledTimes(1);
  });

  it('writes leaf help and returns 0 for a known command', () => {
    const registry = createRegistry(FIXTURE_COMMANDS);
    const groups = commandGroups(registry);
    const { runtime, stdoutWrite } = fakeRuntime();

    const code = runHelpCommand(runtime, FIXTURE_COMMANDS, groups, ['topic', 'list'], OPTIONS);

    expect(code).toBe(0);
    expect(stdoutWrite).toHaveBeenCalledWith(expect.stringContaining('list topics'));
  });

  it('writes group help and returns 0 for a known group', () => {
    const registry = createRegistry(FIXTURE_COMMANDS);
    const groups = commandGroups(registry);
    const { runtime, stdoutWrite } = fakeRuntime();

    const code = runHelpCommand(runtime, FIXTURE_COMMANDS, groups, ['topic'], OPTIONS);

    expect(code).toBe(0);
    expect(stdoutWrite).toHaveBeenCalledWith(expect.stringContaining('topic list'));
  });

  it('writes an error to stderr and returns the usage code for an unknown path', () => {
    const registry = createRegistry(FIXTURE_COMMANDS);
    const groups = commandGroups(registry);
    const { runtime, stderrWrite } = fakeRuntime();

    const code = runHelpCommand(runtime, FIXTURE_COMMANDS, groups, ['nope'], OPTIONS);

    expect(code).toBe(2);
    expect(stderrWrite).toHaveBeenCalledWith(expect.stringContaining('unknown command'));
  });
});
