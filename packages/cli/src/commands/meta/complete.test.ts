import { describe, expect, it } from 'vitest';
import type { CommandSpec } from '../../args/define';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { createCompleteCommand } from './complete';

const FIXTURE: CommandSpec[] = [
  { path: ['topic', 'list'], summary: 'stub', exitCodes: [0], run: async () => 0 },
  { path: ['topic', 'create'], summary: 'stub', exitCodes: [0], run: async () => 0 },
];

describe('createCompleteCommand', () => {
  const completeCommand = createCompleteCommand(() => FIXTURE);

  it('is hidden from help listings', () => {
    expect(completeCommand.hidden).toBe(true);
  });

  it('writes one completion candidate per line and exits ok', async () => {
    const { context, stdoutWrite } = createFakeCommandContext({ positionals: ['to'] });

    const code = await completeCommand.run(context);

    expect(code).toBe(0);
    expect(stdoutWrite).toHaveBeenCalledWith(expect.stringContaining('topic'));
  });

  it('writes the identical list for both human and json formats', async () => {
    const human = createFakeCommandContext({ positionals: ['topic', ''], format: 'human' });
    const json = createFakeCommandContext({ positionals: ['topic', ''], format: 'json' });

    await completeCommand.run(human.context);
    await completeCommand.run(json.context);

    expect(human.stdoutWrite.mock.calls[0]?.[0]).toBe(json.stdoutWrite.mock.calls[0]?.[0]);
  });

  it('never opens an admin connection', async () => {
    const { context, openAdmin } = createFakeCommandContext({ positionals: [''] });

    await completeCommand.run(context);

    expect(openAdmin).not.toHaveBeenCalled();
  });

  it('reads the command list lazily, at run time, not at construction time', async () => {
    let commands: CommandSpec[] = [];
    const lazyCommand = createCompleteCommand(() => commands);
    // The thunk returns an empty array right now — if createCompleteCommand had captured its
    // result eagerly instead of the thunk itself, this reassignment would never be observed.
    commands = FIXTURE;
    const { context, stdoutWrite } = createFakeCommandContext({ positionals: ['to'] });

    await lazyCommand.run(context);

    expect(stdoutWrite).toHaveBeenCalledWith(expect.stringContaining('topic'));
  });
});
