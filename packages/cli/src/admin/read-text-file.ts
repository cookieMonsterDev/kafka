import { readFileSync } from 'node:fs';
import { CliUsageError } from '../args/coerce';

/** Reads a `--from-file`-style flag's path as UTF-8, mapping any failure to a usage error naming the flag. */
export function readTextFile(path: string, flagName: string): string {
  try {
    return readFileSync(path, 'utf8');
  } catch (error) {
    throw new CliUsageError(
      `could not read --${flagName} "${path}": ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}
