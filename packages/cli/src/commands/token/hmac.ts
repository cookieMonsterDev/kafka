import { readStdinToEnd } from '../../admin/read-stdin';
import { CliUsageError } from '../../args/coerce';
import type { Runtime } from '../../runtime';

/**
 * Resolves a delegation token's `hmac` — base64 on `--hmac`, or piped via `--hmac-stdin` so it
 * never lands in shell history or a process list, matching `scram set`'s `--password-stdin`.
 */
export async function resolveHmacFlag(input: {
  hmacFlag: string | undefined;
  hmacStdin: boolean;
  stdin: Runtime['stdin'];
}): Promise<Buffer> {
  if (input.hmacFlag !== undefined && input.hmacStdin) {
    throw new CliUsageError('--hmac and --hmac-stdin are mutually exclusive');
  }

  const raw = input.hmacStdin ? await readStdinToEnd(input.stdin) : input.hmacFlag;
  if (raw === undefined || raw === '') {
    throw new CliUsageError('requires --hmac (base64) or --hmac-stdin');
  }

  const hmac = Buffer.from(raw, 'base64');
  if (hmac.length === 0) {
    throw new CliUsageError(`--hmac must be valid base64, got "${raw}"`);
  }
  return hmac;
}
