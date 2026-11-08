import { READ_ONLY_ADMIN_METHODS, type AdminMethodName } from './admin/method-classification';
import { ALL_COMMANDS } from './commands/index';
import { getCompletions } from './completion/complete';
import { readCliSection } from './config/cli-section';
import type { CommandSpec } from './args/define';
import { dispatch } from './dispatch';
import { CliAbortedError } from './errors/aborted-error';
import type { Runtime } from './runtime';

export type { CommandSpec as EmbeddedCommandSpec } from './args/define';
export { CliUsageError as EmbeddedCliUsageError } from './args/coerce';
export { CliAbortedError as EmbeddedCliAbortedError } from './errors/aborted-error';
export type { Runtime as EmbeddedCliRuntime, ResolvedCliConfig } from './runtime';

const LOCAL_COMMANDS = new Set(['init', 'doctor', 'profiles', 'studio', 'complete', 'completion']);
const BLOCKED_FLAGS = new Set([
  'brokers',
  'client-id',
  'config-file',
  'profile',
  'from-file',
  'password-stdin',
  'hmac-stdin',
  'show-secrets',
  'version',
]);
const COMMANDS = ALL_COMMANDS.filter((command) => !LOCAL_COMMANDS.has(command.path[0] ?? ''));

/** Serializable command metadata for embedded clients, derived from the real CLI registry. */
export const embeddedCommandCatalog = COMMANDS.map((command) => ({
  path: command.path,
  summary: command.summary,
  positionals: command.positionals ?? [],
  flags: (command.flags ?? []).filter((flag) => !BLOCKED_FLAGS.has(flag.name)),
}));

/** Completes CLI command paths, flags and enum values without connecting or loading config. */
export function completeEmbeddedCli(
  words: readonly string[],
  additionalCommands: readonly CommandSpec[] = [],
): string[] {
  return getCompletions([...COMMANDS, ...additionalCommands], words).filter(
    (word) => !BLOCKED_FLAGS.has(word.replace(/^--?/, '')),
  );
}

/**
 * Runs the CLI inside a host-provided connection. Local filesystem/launch commands and connection
 * overrides are unavailable. Read-only enforcement checks actual Admin calls, including passthrough.
 * Destructive operations retain the CLI's confirmation and force checks.
 */
export async function runEmbeddedCli(
  runtime: Runtime,
  options: { readonly readOnly: boolean; readonly additionalCommands?: readonly CommandSpec[] },
): Promise<number> {
  let terminated = false;
  for (const word of runtime.argv) {
    if (word === '--') {
      terminated = true;
      continue;
    }
    if (!terminated && word.startsWith('-') && BLOCKED_FLAGS.has(word.split('=')[0]?.replace(/^--?/, '') ?? '')) {
      runtime.stderr.write(`This terminal uses its host connection; ${word.split('=')[0] ?? word} is unavailable.\n`);
      return 2;
    }
  }
  const guardedRuntime: Runtime = {
    ...runtime,
    openAdmin: async (input) => {
      runtime.signal.throwIfAborted();
      const admin = await runtime.openAdmin(input);
      return new Proxy(admin, {
        get(target, property, receiver): unknown {
          const value: unknown = Reflect.get(target, property, receiver);
          if (typeof value !== 'function') return value;
          return (...args: unknown[]) => {
            if (property !== 'disconnect') {
              runtime.signal.throwIfAborted();
              if (options.readOnly && !READ_ONLY_ADMIN_METHODS.has(property as AdminMethodName)) {
                throw new CliAbortedError(`Studio is in read-only mode; ${String(property)} is unavailable.`);
              }
            }
            const result: unknown = Reflect.apply(value, target, args);
            return result;
          };
        },
      });
    },
  };
  return dispatch(guardedRuntime, [...COMMANDS, ...(options.additionalCommands ?? [])]);
}

/** Uses already-loaded host configuration without rediscovering files or reading environment variables. */
export function embeddedCliConfig(
  fileConfig: Record<string, unknown> | null,
  profile: string | null,
  path: string | null = null,
): import('./config/resolve').ResolvedCliConfig {
  return { path, fileConfig, cli: readCliSection(fileConfig), profile, transformFallbackUsed: false };
}
