import type { Runtime } from '../runtime';

/**
 * Reads `stdin` to EOF and trims exactly one trailing newline — for a secret piped in rather than
 * passed as a flag (shell history, `ps`, and shared shells all expose a plain `--password value`).
 * Mirrors `interaction/confirm.ts`'s `readLine`, but reads to `end` instead of stopping at the
 * first newline, since a piped secret is the whole stream, not one interactive answer.
 */
export function readStdinToEnd(stdin: Runtime['stdin']): Promise<string> {
  return new Promise((resolve) => {
    let buffer = '';
    stdin.setEncoding('utf8');
    stdin.on('data', (chunk) => {
      buffer += chunk;
    });
    stdin.on('end', () => {
      resolve(buffer.endsWith('\n') ? buffer.slice(0, -1) : buffer);
    });
  });
}
