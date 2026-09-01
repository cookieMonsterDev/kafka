import { readFileSync } from 'node:fs';
import { CliUsageError } from '../args/coerce';

/** Reads and JSON-parses a `--from-file`-style flag's path, mapping any failure to a usage error naming the flag. */
export function readJsonFile(path: string, flagName: string): unknown {
  let text: string;
  try {
    text = readFileSync(path, 'utf8');
  } catch (error) {
    throw new CliUsageError(
      `could not read --${flagName} "${path}": ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  try {
    return JSON.parse(text);
  } catch (error) {
    throw new CliUsageError(
      `--${flagName} "${path}" is not valid JSON: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}
