/**
 * Raised for a CLI-owned config concern that never touches `@cookiemonsterdev/kafka-core` at all
 * — an explicit `--config`/`KAFKA_CONFIG` path that doesn't exist, or an unknown
 * `--profile`/`KAFKA_PROFILE` name. Distinct from core's own `KafkaConfigError` (raised once a
 * connection is actually attempted), but maps to the same exit code.
 */
export class CliConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CliConfigError';
  }
}
