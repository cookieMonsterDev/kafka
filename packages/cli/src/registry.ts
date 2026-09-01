import type { CommandSpec } from './args/define';
import { EXIT_CODES } from './errors/exit-codes';

export class CommandRegistrationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CommandRegistrationError';
  }
}

/** Flag names every command shares (help/version/output/verbosity/color) — no command may reuse one. */
export const RESERVED_FLAG_NAMES: ReadonlySet<string> = new Set([
  'help',
  'version',
  'json',
  'format',
  'quiet',
  'verbose',
  'color',
  'config-file',
  'profile',
]);

const VALID_EXIT_CODES: ReadonlySet<number> = new Set(Object.values(EXIT_CODES));

function pathKey(path: readonly string[]): string {
  return path.join(' ');
}

/**
 * Builds the flat `"topic create"` → {@link CommandSpec} map every other module routes through.
 * Validated once, at import time, so a bad mount fails `pnpm test` rather than a user's terminal:
 * a duplicate path, a flag name reserved for global use, a reused single-letter alias within one
 * command, or an exit code outside the shared taxonomy all throw here.
 */
export function createRegistry(commands: readonly CommandSpec[]): ReadonlyMap<string, CommandSpec> {
  const registry = new Map<string, CommandSpec>();

  for (const command of commands) {
    const key = pathKey(command.path);
    if (registry.has(key)) {
      throw new CommandRegistrationError(`duplicate command path "${key}"`);
    }

    const seenNames = new Set<string>();
    const seenAliases = new Set<string>();
    for (const flag of command.flags ?? []) {
      if (RESERVED_FLAG_NAMES.has(flag.name)) {
        throw new CommandRegistrationError(`"${key}" declares --${flag.name}, which is reserved for global use`);
      }
      if (seenNames.has(flag.name)) {
        throw new CommandRegistrationError(`"${key}" declares --${flag.name} more than once`);
      }
      seenNames.add(flag.name);

      if (flag.alias !== undefined) {
        if (seenAliases.has(flag.alias)) {
          throw new CommandRegistrationError(`"${key}" reuses alias -${flag.alias} on more than one flag`);
        }
        seenAliases.add(flag.alias);
      }
    }

    for (const code of command.exitCodes) {
      if (!VALID_EXIT_CODES.has(code)) {
        throw new CommandRegistrationError(
          `"${key}" declares exit code ${String(code)}, which is not in the shared taxonomy`,
        );
      }
    }

    registry.set(key, command);
  }

  return registry;
}

/** Every command path prefix reachable in the registry, e.g. `"topic"` for `"topic list"`. */
export function commandGroups(registry: ReadonlyMap<string, CommandSpec>): ReadonlySet<string> {
  const groups = new Set<string>();
  for (const key of registry.keys()) {
    const parts = key.split(' ');
    for (let i = 1; i < parts.length; i++) {
      groups.add(parts.slice(0, i).join(' '));
    }
  }
  return groups;
}
