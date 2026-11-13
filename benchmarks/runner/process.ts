import { spawn } from 'node:child_process';

export interface CommandOptions {
  cwd?: string;
  env?: NodeJS.ProcessEnv;
  /** Written to the child's stdin, which is then closed. */
  input?: string;
  timeoutMs?: number;
  /** Resolve with the exit code instead of throwing `CommandError` on a non-zero exit. */
  allowFailure?: boolean;
  /** Called for every complete stdout and stderr line while the command runs. */
  onLine?: (line: string) => void;
  /** Keep only this many trailing characters of each stream (default 4 MiB). */
  maxCapturedChars?: number;
}

export interface CommandResult {
  exitCode: number | null;
  stdout: string;
  stderr: string;
}

export class CommandError extends Error {
  readonly exitCode: number | null;
  readonly stdout: string;
  readonly stderr: string;

  constructor(message: string, result: CommandResult) {
    super(message);
    this.name = 'CommandError';
    this.exitCode = result.exitCode;
    this.stdout = result.stdout;
    this.stderr = result.stderr;
  }
}

const defaultMaxCapturedChars = 4 * 1024 * 1024;

/** The message of an `Error`, or the value as a string for anything else that was thrown. */
export function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export function tailLines(text: string, lineCount: number): string {
  return text.trimEnd().split('\n').slice(-lineCount).join('\n');
}

function appendBounded(buffer: string, chunk: string, maxChars: number): string {
  const combined = buffer + chunk;
  if (combined.length <= maxChars) {
    return combined;
  }
  return combined.slice(combined.length - maxChars);
}

/**
 * Runs a command without a shell and collects its output. Rejects with `CommandError` on a timeout,
 * and on a non-zero exit unless `allowFailure` is set.
 */
export function runCommand(
  command: string,
  args: readonly string[],
  options: CommandOptions = {},
): Promise<CommandResult> {
  const commandLine = [command, ...args].join(' ');
  const maxChars = options.maxCapturedChars ?? defaultMaxCapturedChars;
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: options.cwd,
      env: options.env ?? process.env,
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    const pendingLines = { stdout: '', stderr: '' };
    let isTimedOut = false;
    const timer =
      options.timeoutMs === undefined
        ? undefined
        : setTimeout(() => {
            isTimedOut = true;
            child.kill('SIGKILL');
          }, options.timeoutMs);

    const forwardLines = (stream: 'stdout' | 'stderr', chunk: string): void => {
      if (options.onLine === undefined) {
        return;
      }
      const lines = (pendingLines[stream] + chunk).split('\n');
      pendingLines[stream] = lines.pop() ?? '';
      for (const line of lines) {
        options.onLine(line);
      }
    };

    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');
    child.stdout.on('data', (chunk: string) => {
      stdout = appendBounded(stdout, chunk, maxChars);
      forwardLines('stdout', chunk);
    });
    child.stderr.on('data', (chunk: string) => {
      stderr = appendBounded(stderr, chunk, maxChars);
      forwardLines('stderr', chunk);
    });
    child.on('error', (error) => {
      clearTimeout(timer);
      reject(new Error(`Cannot start ${commandLine}: ${error.message}`, { cause: error }));
    });
    child.on('close', (exitCode) => {
      clearTimeout(timer);
      for (const rest of [pendingLines.stdout, pendingLines.stderr]) {
        if (rest.length > 0) {
          options.onLine?.(rest);
        }
      }
      const result: CommandResult = { exitCode, stdout, stderr };
      if (isTimedOut) {
        reject(new CommandError(`${commandLine} timed out after ${options.timeoutMs} ms`, result));
        return;
      }
      if (exitCode !== 0 && options.allowFailure !== true) {
        const output = tailLines(result.stderr || result.stdout, 10);
        reject(new CommandError(`${commandLine} exited with ${exitCode}: ${output}`, result));
        return;
      }
      resolve(result);
    });

    child.stdin.on('error', () => {
      // The child may exit before reading its input; the exit code reports that failure.
    });
    child.stdin.end(options.input ?? '');
  });
}

/** Runs a Docker CLI command and returns stdout; throws with the command and stderr tail on failure. */
export async function docker(args: readonly string[], options: CommandOptions = {}): Promise<string> {
  const result = await runCommand('docker', args, { ...options, allowFailure: true });
  if (result.exitCode !== 0) {
    const output = tailLines(result.stderr || result.stdout, 40);
    throw new Error(`docker ${args.slice(0, 3).join(' ')} exited with ${String(result.exitCode)}:\n${output}`);
  }
  return result.stdout;
}
