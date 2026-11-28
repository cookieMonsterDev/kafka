import { CliUsageError } from '../args/coerce';
import { readTextFile } from './read-text-file';

/** Reads and JSON-parses a `--from-file`-style flag's path, mapping any failure to a usage error naming the flag. */
export function readJsonFile(path: string, flagName: string): unknown {
  const text = readTextFile(path, flagName);
  try {
    return JSON.parse(text);
  } catch (error) {
    throw new CliUsageError(
      `--${flagName} "${path}" is not valid JSON: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}
