import { main } from '../../src/main';
import { createRuntime, type RuntimeProcessLike } from '../../src/runtime';

export interface CliResult {
  readonly code: number;
  readonly stdout: string;
  readonly stderr: string;
}

/** Runs the real CLI end to end — real command routing, real openAdmin, real broker. */
export async function runCli(
  argv: readonly string[],
  env: Record<string, string | undefined> = {},
): Promise<CliResult> {
  let stdout = '';
  let stderr = '';

  const fakeProcess: RuntimeProcessLike = {
    argv: ['node', 'kafka', ...argv],
    env: { ...process.env, ...env },
    cwd: () => process.cwd(),
    stdout: {
      write: (chunk: string) => {
        stdout += chunk;
        return true;
      },
    },
    stderr: {
      write: (chunk: string) => {
        stderr += chunk;
        return true;
      },
    },
    stdin: { isTTY: false, setEncoding: () => undefined, on: () => undefined },
    on: () => undefined,
  };

  const code = await main(createRuntime(fakeProcess));
  return { code, stdout, stderr };
}
