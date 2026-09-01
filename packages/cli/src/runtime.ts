import type { OnConfigDiagnostic } from '@cookiemonsterdev/kafka-config';
import { openAdmin, type OpenAdmin } from './admin/open';
import { resolveCliConfig, type ResolvedCliConfig } from './config/resolve';

/** Minimal writable stream contract — enough for `process.stdout`/`process.stderr`. */
export interface RuntimeWriter {
  write(chunk: string): boolean;
  readonly isTTY?: boolean;
  readonly columns?: number;
}

/** Minimal readable stream contract — enough for `process.stdin`. */
export interface RuntimeReader {
  readonly isTTY?: boolean;
  setEncoding(encoding: BufferEncoding): unknown;
  on(event: 'data', listener: (chunk: string) => void): unknown;
  on(event: 'end', listener: () => void): unknown;
}

export type { OpenAdmin, OpenAdminOptions } from './admin/port';
export type { ResolvedCliConfig } from './config/resolve';

export interface LoadConfigOptions {
  readonly configFlag?: string;
  readonly profileFlag?: string;
  readonly onDiagnostic?: OnConfigDiagnostic;
  readonly onWarn?: (message: string) => void;
}

/** Resolves this invocation's `kafka.config.*` file — see `config/resolve.ts` for what that means. */
export type LoadConfig = (options: LoadConfigOptions) => Promise<ResolvedCliConfig>;

/**
 * Everything a command touches instead of the ambient `process` global, so every command is
 * testable by constructing a plain object — no real process, no real broker.
 */
export interface Runtime {
  readonly argv: readonly string[];
  readonly env: Readonly<Record<string, string | undefined>>;
  readonly cwd: string;
  readonly stdout: RuntimeWriter;
  readonly stderr: RuntimeWriter;
  readonly stdin: RuntimeReader;
  readonly isTty: boolean;
  readonly columns: number;
  now(): Date;
  /**
   * Defensive escape hatch, not a normal exit path: a command reports its outcome by returning
   * an exit code, never by calling this. Calling it always throws, so an accidental call fails
   * loudly (in tests and in production) instead of silently terminating the process out from
   * under whatever cleanup the caller expected to run.
   */
  exit(code: number): never;
  readonly openAdmin: OpenAdmin;
  readonly loadConfig: LoadConfig;
  readonly signal: AbortSignal;
}

export class RuntimeExitCalledError extends Error {
  readonly code: number;

  constructor(code: number) {
    super(`runtime.exit(${String(code)}) was called directly instead of returning an exit code`);
    this.code = code;
  }
}

/** The subset of `NodeJS.Process` the runtime actually reads — narrow so a test fake stays small. */
export interface RuntimeProcessLike {
  readonly argv: readonly string[];
  readonly env: Readonly<Record<string, string | undefined>>;
  cwd(): string;
  readonly stdout: RuntimeWriter;
  readonly stderr: RuntimeWriter;
  readonly stdin: RuntimeReader;
  on(event: 'SIGINT' | 'SIGTERM', listener: () => void): unknown;
}

export function createRuntime(proc: RuntimeProcessLike): Runtime {
  const controller = new AbortController();
  proc.on('SIGINT', () => controller.abort('SIGINT'));
  proc.on('SIGTERM', () => controller.abort('SIGTERM'));

  const cwd = proc.cwd();

  return {
    argv: proc.argv.slice(2),
    env: proc.env,
    cwd,
    stdout: proc.stdout,
    stderr: proc.stderr,
    stdin: proc.stdin,
    isTty: proc.stdout.isTTY === true,
    columns: proc.stdout.columns ?? 80,
    now: () => new Date(),
    exit(code: number): never {
      throw new RuntimeExitCalledError(code);
    },
    openAdmin,
    loadConfig: (options) => resolveCliConfig({ cwd, env: proc.env, ...options }),
    signal: controller.signal,
  };
}
