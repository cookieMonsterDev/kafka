import { openAdmin, type OpenAdmin } from './admin/open';

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

/**
 * Placeholder for reading a `kafka.config.*` file — not wired up yet. Every field the config
 * file could someday supply defaults to `undefined`, so a command written against this today
 * behaves identically once it is implemented.
 */
export type LoadConfig = () => Promise<Record<string, unknown>>;

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

  return {
    argv: proc.argv.slice(2),
    env: proc.env,
    cwd: proc.cwd(),
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
    loadConfig: async () => ({}),
    signal: controller.signal,
  };
}
