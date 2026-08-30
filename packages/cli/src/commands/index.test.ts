import { describe, expect, it } from 'vitest';
import { parseCommandArgs } from '../args/parse';
import { createRegistry } from '../registry';
import { ALL_COMMANDS } from './index';

describe('every mounted command', () => {
  it('mounts without a registration error', () => {
    expect(() => createRegistry(ALL_COMMANDS)).not.toThrow();
  });

  it.each(ALL_COMMANDS)('"$path" has a non-empty summary', (command) => {
    expect(command.summary.length).toBeGreaterThan(0);
  });

  it.each(ALL_COMMANDS.flatMap((command) => (command.flags ?? []).map((flag) => ({ command, flag }))))(
    '"$command.path" flag --$flag.name has a non-empty brief',
    ({ flag }) => {
      expect(flag.brief.length).toBeGreaterThan(0);
    },
  );

  it.each(ALL_COMMANDS.flatMap((command) => (command.examples ?? []).map((example) => ({ command, example }))))(
    'the documented example "$example" re-parses without a usage error',
    ({ command, example }) => {
      const tokens = example.split(' ').slice(command.path.length);
      expect(() => parseCommandArgs(tokens, command.flags ?? [], command.positionals ?? [])).not.toThrow();
    },
  );
});
