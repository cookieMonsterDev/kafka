import { describe, expect, it } from 'vitest';
import type { CommandSpec } from './args/define';
import { EXIT_CODES } from './errors/exit-codes';
import { commandGroups, CommandRegistrationError, createRegistry } from './registry';

function command(overrides: Partial<CommandSpec> & Pick<CommandSpec, 'path'>): CommandSpec {
  return {
    summary: 'test command',
    exitCodes: [EXIT_CODES.ok],
    run: async () => EXIT_CODES.ok,
    ...overrides,
  };
}

describe('createRegistry', () => {
  it('maps a command by its space-joined path', () => {
    const registry = createRegistry([command({ path: ['topic', 'list'] })]);
    expect(registry.has('topic list')).toBe(true);
    expect(registry.get('topic list')?.summary).toBe('test command');
  });

  it('throws on a duplicate path', () => {
    expect(() => createRegistry([command({ path: ['topic', 'list'] }), command({ path: ['topic', 'list'] })])).toThrow(
      CommandRegistrationError,
    );
  });

  it('throws when a command declares a reserved flag name', () => {
    expect(() =>
      createRegistry([command({ path: ['topic', 'list'], flags: [{ name: 'json', type: 'boolean', brief: '' }] })]),
    ).toThrow(/reserved for global use/);
  });

  it('throws when a command declares the same flag name twice', () => {
    expect(() =>
      createRegistry([
        command({
          path: ['topic', 'list'],
          flags: [
            { name: 'wait', type: 'boolean', brief: '' },
            { name: 'wait', type: 'boolean', brief: '' },
          ],
        }),
      ]),
    ).toThrow(CommandRegistrationError);
  });

  it('throws when a command reuses an alias across two flags', () => {
    expect(() =>
      createRegistry([
        command({
          path: ['topic', 'create'],
          flags: [
            { name: 'partitions', type: 'number', alias: 'p', brief: '' },
            { name: 'path', type: 'string', alias: 'p', brief: '' },
          ],
        }),
      ]),
    ).toThrow(/reuses alias/);
  });

  it('throws when a command declares an exit code outside the shared taxonomy', () => {
    expect(() => createRegistry([command({ path: ['topic', 'list'], exitCodes: [42] })])).toThrow(
      /not in the shared taxonomy/,
    );
  });

  it('accepts two commands that share an alias in different commands', () => {
    const registry = createRegistry([
      command({ path: ['topic', 'create'], flags: [{ name: 'partitions', type: 'number', alias: 'p', brief: '' }] }),
      command({ path: ['topic', 'list'], flags: [{ name: 'prefix', type: 'string', alias: 'p', brief: '' }] }),
    ]);
    expect(registry.size).toBe(2);
  });
});

describe('commandGroups', () => {
  it('derives every path prefix', () => {
    const registry = createRegistry([
      command({ path: ['topic', 'list'] }),
      command({ path: ['topic', 'create'] }),
      command({ path: ['ping'] }),
    ]);
    expect(commandGroups(registry)).toEqual(new Set(['topic']));
  });
});
