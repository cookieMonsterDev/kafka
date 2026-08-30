import { describe, expect, it } from 'vitest';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { adminMethodsCommand } from './methods';

describe('adminMethodsCommand', () => {
  it('lists every method as a human table', async () => {
    const { context, stdoutWrite } = createFakeCommandContext({});
    const code = await adminMethodsCommand.run(context);

    expect(code).toBe(0);
    expect(stdoutWrite).toHaveBeenCalledWith(expect.stringContaining('listTopics'));
    expect(stdoutWrite).toHaveBeenCalledWith(expect.stringContaining('mounted'));
    expect(stdoutWrite).toHaveBeenCalledWith(expect.stringContaining('passthrough-only'));
  });

  it('emits every method in JSON with its classification and read-only flag', async () => {
    const { context, stdoutWrite } = createFakeCommandContext({ format: 'json' });
    await adminMethodsCommand.run(context);

    const written = stdoutWrite.mock.calls[0]?.[0] ?? '';
    const parsed = JSON.parse(written) as { methods: { name: string; classification: string; readOnly: boolean }[] };
    const listTopics = parsed.methods.find((m) => m.name === 'listTopics');
    expect(listTopics).toEqual({ name: 'listTopics', classification: 'mounted', readOnly: true });
    const createTopics = parsed.methods.find((m) => m.name === 'createTopics');
    expect(createTopics?.readOnly).toBe(false);
  });
});
