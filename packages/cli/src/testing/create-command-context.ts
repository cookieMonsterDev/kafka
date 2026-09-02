import { vi } from 'vitest';
import type { CommandContext } from '../args/define';
import type { ResolvedCliConfig } from '../config/resolve';
import { createCommandOutput } from '../output/format';
import { CLI_LOG_LEVELS } from '../output/logger';
import type { Runtime } from '../runtime';

export interface FakeCommandContext {
  readonly context: CommandContext;
  readonly stdoutWrite: ReturnType<typeof createWriteSpy>;
  readonly stderrWrite: ReturnType<typeof createWriteSpy>;
  readonly openAdmin: ReturnType<typeof vi.fn>;
}

function createWriteSpy() {
  return vi.fn((_chunk: string) => true);
}

export const EMPTY_RESOLVED_CLI_CONFIG: ResolvedCliConfig = Object.freeze({
  path: null,
  fileConfig: null,
  cli: {},
  profile: null,
  transformFallbackUsed: false,
});

export function createFakeCommandContext(input: {
  flags?: Record<string, unknown>;
  positionals?: string[];
  openAdmin?: Runtime['openAdmin'];
  format?: 'human' | 'json';
  config?: ResolvedCliConfig;
  env?: Record<string, string | undefined>;
  cwd?: string;
  stdin?: Runtime['stdin'];
}): FakeCommandContext {
  const stdoutWrite = createWriteSpy();
  const stderrWrite = createWriteSpy();
  const openAdmin = vi.fn(
    input.openAdmin ??
      (async () => {
        throw new Error('openAdmin not stubbed');
      }),
  );
  const config = input.config ?? EMPTY_RESOLVED_CLI_CONFIG;

  const runtime = {
    argv: [],
    env: input.env ?? {},
    cwd: input.cwd ?? '/work',
    stdout: { write: stdoutWrite },
    stderr: { write: stderrWrite },
    stdin: input.stdin ?? { setEncoding: vi.fn(), on: vi.fn() },
    isTty: false,
    columns: 80,
    now: () => new Date(),
    exit: () => {
      throw new Error('exit() should not be called from a command');
    },
    openAdmin,
    loadConfig: vi.fn(async () => config),
    signal: new AbortController().signal,
  } as unknown as Runtime;

  const output = createCommandOutput({
    stdout: { write: stdoutWrite },
    stderr: { write: stderrWrite },
    format: input.format ?? 'human',
    useColor: false,
    logLevel: CLI_LOG_LEVELS.WARN,
  });

  const context: CommandContext = {
    runtime,
    flags: input.flags ?? {},
    positionals: input.positionals ?? [],
    output,
    config,
  };

  return { context, stdoutWrite, stderrWrite, openAdmin };
}
