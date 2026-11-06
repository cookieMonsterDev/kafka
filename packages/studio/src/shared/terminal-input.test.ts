import { describe, expect, it } from 'vitest';
import { terminalArgv, tokenizeTerminalInput } from './terminal-input';

describe('terminal command input', () => {
  it('accepts the optional CLI prefix, quotes and end-of-options marker', () => {
    expect(terminalArgv('kafka group describe "payments worker"')).toEqual(['group', 'describe', 'payments worker']);
    expect(terminalArgv('group describe -- --help')).toEqual(['group', 'describe', '--', '--help']);
  });
  it.each(['with space', 'with"quote', 'with\\slash', 'with\ttab', 'with\nline'])(
    'preserves JSON-quoted identifiers: %s',
    (name) => {
      expect(terminalArgv(`group describe ${JSON.stringify(name)}`)[2]).toBe(name);
    },
  );
  it('tolerates incomplete input for completion but rejects it for execution', () => {
    expect(tokenizeTerminalInput('group describe "unfinished', false).at(-1)?.value).toBe('unfinished');
    expect(() => terminalArgv('group describe "unfinished')).toThrow('Close the quoted');
  });
});
