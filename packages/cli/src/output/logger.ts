/**
 * A local copy of core's `LOG_LEVELS`, not an import of it: this module runs on every
 * invocation, including `--help`/`--version`, and core is meant to be loaded only inside a
 * command that actually connects. A test asserts this stays deep-equal to core's real export.
 */
export const CLI_LOG_LEVELS = Object.freeze({
  NOTHING: 0,
  ERROR: 1,
  WARN: 2,
  INFO: 4,
  DEBUG: 5,
});

export type CliLogLevel = (typeof CLI_LOG_LEVELS)[keyof typeof CLI_LOG_LEVELS];

/** `-q` silences everything but errors; each `-v` raises detail by one step. */
export function verbosityToLogLevel(quiet: boolean, verboseCount: number): CliLogLevel {
  if (quiet) return CLI_LOG_LEVELS.ERROR;
  if (verboseCount >= 2) return CLI_LOG_LEVELS.DEBUG;
  if (verboseCount === 1) return CLI_LOG_LEVELS.INFO;
  return CLI_LOG_LEVELS.WARN;
}

export interface CliLogger {
  warn(message: string): void;
  info(message: string): void;
  debug(message: string): void;
}

/** Every level writes to stderr — stdout is reserved for a command's actual data. */
export function createLogger(stderr: { write(chunk: string): unknown }, level: CliLogLevel): CliLogger {
  return {
    warn(message) {
      if (level >= CLI_LOG_LEVELS.WARN) stderr.write(`${message}\n`);
    },
    info(message) {
      if (level >= CLI_LOG_LEVELS.INFO) stderr.write(`${message}\n`);
    },
    debug(message) {
      if (level >= CLI_LOG_LEVELS.DEBUG) stderr.write(`${message}\n`);
    },
  };
}
