import { describe, expect, it } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { completionCommand } from './completion';

describe('completionCommand', () => {
  it('is visible (not hidden) — a person is meant to run this one directly', () => {
    expect(completionCommand.hidden).toBeUndefined();
  });

  it.each(['bash', 'zsh', 'fish'])('emits a script naming kafka complete for %s', async (shell) => {
    const { context, stdoutWrite } = createFakeCommandContext({ positionals: [shell] });

    const code = await completionCommand.run(context);

    expect(code).toBe(0);
    expect(stdoutWrite).toHaveBeenCalledWith(expect.stringContaining('kafka complete --'));
  });

  it('rejects an unknown shell with a usage error', async () => {
    const { context } = createFakeCommandContext({ positionals: ['powershell'] });

    await expect(completionCommand.run(context)).rejects.toThrow(CliUsageError);
  });

  it('rejects a missing shell argument with a usage error', async () => {
    const { context } = createFakeCommandContext({ positionals: [] });

    await expect(completionCommand.run(context)).rejects.toThrow(CliUsageError);
  });
});
