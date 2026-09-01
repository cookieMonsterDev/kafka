import { describe, expect, it } from 'vitest';
import { EXIT_CODES } from './exit-codes';
import { mapKafkaError } from './map-kafka-error';

function fakeError(name: string, message = 'boom', extra: Record<string, unknown> = {}): unknown {
  return { name, message, ...extra };
}

describe('mapKafkaError', () => {
  it('maps CliUsageError to the usage exit code', () => {
    expect(mapKafkaError(fakeError('CliUsageError')).exitCode).toBe(EXIT_CODES.usage);
  });

  it('maps KafkaConfigError and KafkaConfigRequiresAsyncError to the config exit code', () => {
    expect(mapKafkaError(fakeError('KafkaConfigError')).exitCode).toBe(EXIT_CODES.config);
    expect(mapKafkaError(fakeError('KafkaConfigRequiresAsyncError')).exitCode).toBe(EXIT_CODES.config);
  });

  it("maps the CLI's own CliConfigError to the config exit code", () => {
    expect(mapKafkaError(fakeError('CliConfigError')).exitCode).toBe(EXIT_CODES.config);
  });

  it('maps KafkaServerDoesNotSupportApiKey to the unsupported-by-broker exit code', () => {
    expect(mapKafkaError(fakeError('KafkaServerDoesNotSupportApiKey')).exitCode).toBe(EXIT_CODES.unsupportedByBroker);
  });

  it('maps KafkaSASLAuthenticationError to the auth-failed exit code', () => {
    expect(mapKafkaError(fakeError('KafkaSASLAuthenticationError')).exitCode).toBe(EXIT_CODES.authFailed);
  });

  it('maps an unrecognized Kafka* error to the generic operation-failed exit code', () => {
    expect(mapKafkaError(fakeError('KafkaBrokerNotFound')).exitCode).toBe(EXIT_CODES.operationFailed);
  });

  it('flattens a KafkaAggregateError into items', () => {
    const error = fakeError('KafkaAggregateError', 'two topics failed', {
      errors: [fakeError('KafkaCreateTopicError', 'orders exists'), { message: 'payments: timeout' }],
    });
    const mapped = mapKafkaError(error);
    expect(mapped.exitCode).toBe(EXIT_CODES.operationFailed);
    expect(mapped.message).toBe('two topics failed');
    expect(mapped.items).toEqual([{ message: 'orders exists' }, { message: 'payments: timeout' }]);
  });

  it('maps an unrecognized non-Kafka throw to the internal-bug exit code with a bug-report line', () => {
    const mapped = mapKafkaError(new TypeError('cannot read x of undefined'));
    expect(mapped.exitCode).toBe(EXIT_CODES.internalBug);
    expect(mapped.message).toContain('cannot read x of undefined');
    expect(mapped.message).toMatch(/report it/i);
  });

  it('maps a thrown string to the internal-bug exit code without crashing', () => {
    const mapped = mapKafkaError('a plain string throw');
    expect(mapped.exitCode).toBe(EXIT_CODES.internalBug);
    expect(mapped.message).toContain('a plain string throw');
  });

  it('never uses instanceof — a differently-constructed object with the right name and message maps identically', () => {
    class NotReallyCoreError {
      name = 'KafkaSASLAuthenticationError';
      message = 'bad credentials';
    }
    expect(mapKafkaError(new NotReallyCoreError()).exitCode).toBe(EXIT_CODES.authFailed);
  });
});
