/** Minimal writable stream contract — enough for `process.stdout`/`process.stderr`. */
export interface RuntimeWriter {
  write(chunk: string): boolean;
}

/**
 * Everything the server touches instead of the ambient `process` global, so the whole studio
 * process is testable by constructing a plain object — no real process, no real sockets. Mirrors
 * `@cookiemonsterdev/kafka-cli`'s own `Runtime` port.
 */
export interface Runtime {
  readonly argv: readonly string[];
  readonly cwd: string;
  readonly env: Readonly<Record<string, string | undefined>>;
  readonly platform: NodeJS.Platform;
  readonly stdout: RuntimeWriter;
  readonly stderr: RuntimeWriter;
  now(): Date;
  /**
   * Defensive escape hatch, not a normal exit path: `main` reports its outcome by returning an
   * exit code, never by calling this. Calling it always throws, so an accidental call fails
   * loudly instead of silently terminating the process out from under an open server.
   */
  exit(code: number): never;
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
  readonly platform: NodeJS.Platform;
  readonly stdout: RuntimeWriter;
  readonly stderr: RuntimeWriter;
  cwd(): string;
  on(event: 'SIGINT' | 'SIGTERM', listener: () => void): unknown;
}

export function createRuntime(proc: RuntimeProcessLike): Runtime {
  const controller = new AbortController();
  proc.on('SIGINT', () => controller.abort('SIGINT'));
  proc.on('SIGTERM', () => controller.abort('SIGTERM'));

  return {
    argv: proc.argv.slice(2),
    cwd: proc.cwd(),
    env: proc.env,
    platform: proc.platform,
    stdout: proc.stdout,
    stderr: proc.stderr,
    now: () => new Date(),
    exit(code: number): never {
      throw new RuntimeExitCalledError(code);
    },
    signal: controller.signal,
  };
}
