import type { CliError } from './cli-error';
import { EXIT_CODES } from './exit-codes';

/**
 * Every thrown value this maps by `.name`, never `instanceof` — the config file (and, later,
 * anything a user's own code throws through it) may come from a different installed copy of
 * core than the CLI's own, so an `instanceof` check would silently miss it.
 */
const EXIT_CODE_BY_NAME: Readonly<Record<string, number>> = {
  CliUsageError: EXIT_CODES.usage,
  KafkaConfigError: EXIT_CODES.config,
  KafkaConfigRequiresAsyncError: EXIT_CODES.config,
  KafkaServerDoesNotSupportApiKey: EXIT_CODES.unsupportedByBroker,
  KafkaSASLAuthenticationError: EXIT_CODES.authFailed,
};

function hasStringProperty<K extends string>(value: object, key: K): value is Record<K, string> {
  return key in value && typeof (value as Record<string, unknown>)[key] === 'string';
}

function messageOf(error: unknown): string {
  if (typeof error === 'object' && error !== null && hasStringProperty(error, 'message')) return error.message;
  return String(error);
}

function nameOf(error: unknown): string | undefined {
  if (typeof error === 'object' && error !== null && hasStringProperty(error, 'name')) return error.name;
  return undefined;
}

/** Maps anything a command's `run()` throws to an exit code and a message the CLI can print. */
export function mapKafkaError(error: unknown): CliError {
  const name = nameOf(error);

  if (name === 'KafkaAggregateError' && typeof error === 'object' && error !== null && 'errors' in error) {
    const errors = (error as { errors: readonly unknown[] }).errors;
    return {
      exitCode: EXIT_CODES.operationFailed,
      message: messageOf(error),
      items: errors.map((item) => ({ message: messageOf(item) })),
    };
  }

  if (name !== undefined) {
    const exitCode = EXIT_CODE_BY_NAME[name];
    if (exitCode !== undefined) {
      return { exitCode, message: messageOf(error) };
    }
    if (name.startsWith('Kafka')) {
      return { exitCode: EXIT_CODES.operationFailed, message: messageOf(error) };
    }
  }

  return {
    exitCode: EXIT_CODES.internalBug,
    message: `${messageOf(error)}\nThis looks like a bug in the CLI — please report it with the command you ran.`,
  };
}
