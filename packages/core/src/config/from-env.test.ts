import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { LOG_LEVELS } from '../loggers/index';
import { defaultOnFromEnvDiagnostic, fromEnv, type FromEnvDiagnostic } from './from-env';

function collect() {
  const diagnostics: FromEnvDiagnostic[] = [];
  return { diagnostics, onDiagnostic: (diagnostic: FromEnvDiagnostic) => diagnostics.push(diagnostic) };
}

describe('fromEnv', () => {
  it('returns {} for an empty env', () => {
    expect(fromEnv({})).toEqual({});
  });

  it('parses KAFKA_BROKERS as a trimmed, comma-separated list, dropping empty entries', () => {
    const { diagnostics, onDiagnostic } = collect();

    const config = fromEnv({ KAFKA_BROKERS: ' a:1 , ,b:2 ' }, { onDiagnostic });

    expect(config.brokers).toEqual(['a:1', 'b:2']);
    expect(diagnostics).toEqual([]);
  });

  it('passes KAFKA_CLIENT_ID through unchanged', () => {
    expect(fromEnv({ KAFKA_CLIENT_ID: 'my-app' })).toEqual({ clientId: 'my-app' });
  });

  it('omits KAFKA_CONNECTION_TIMEOUT and warns when it is not a number, never NaN', () => {
    const { diagnostics, onDiagnostic } = collect();

    const config = fromEnv({ KAFKA_CONNECTION_TIMEOUT: 'abc' }, { onDiagnostic });

    expect(config).not.toHaveProperty('connectionTimeout');
    expect(config.connectionTimeout).not.toBeNaN();
    expect(diagnostics).toHaveLength(1);
    expect(diagnostics[0]).toMatchObject({
      code: 'config.env-invalid',
      level: 'warn',
      key: 'KAFKA_CONNECTION_TIMEOUT',
    });
  });

  it('parses KAFKA_CONNECTION_TIMEOUT and KAFKA_REQUEST_TIMEOUT as numbers', () => {
    const config = fromEnv({ KAFKA_CONNECTION_TIMEOUT: '5000', KAFKA_REQUEST_TIMEOUT: '30000' });

    expect(config.connectionTimeout).toBe(5000);
    expect(config.requestTimeout).toBe(30000);
  });

  it('parses KAFKA_LOG_LEVEL by name, case-insensitively, and it survives a merge with 0', () => {
    expect(fromEnv({ KAFKA_LOG_LEVEL: 'nothing' }).logLevel).toBe(0);
    expect(fromEnv({ KAFKA_LOG_LEVEL: 'nothing' }).logLevel).toBe(LOG_LEVELS.NOTHING);
    expect(fromEnv({ KAFKA_LOG_LEVEL: 'DEBUG' }).logLevel).toBe(LOG_LEVELS.DEBUG);
  });

  it('omits and warns on an unrecognized KAFKA_LOG_LEVEL', () => {
    const { diagnostics, onDiagnostic } = collect();

    const config = fromEnv({ KAFKA_LOG_LEVEL: 'verbose' }, { onDiagnostic });

    expect(config).not.toHaveProperty('logLevel');
    expect(diagnostics).toHaveLength(1);
    expect(diagnostics[0]?.key).toBe('KAFKA_LOG_LEVEL');
  });

  it('builds sasl from a supported mechanism plus username/password', () => {
    const config = fromEnv({
      KAFKA_SASL_MECHANISM: 'PLAIN',
      KAFKA_SASL_USERNAME: 'alice',
      KAFKA_SASL_PASSWORD: 'secret',
    });

    expect(config.sasl).toEqual({ mechanism: 'plain', username: 'alice', password: 'secret' });
  });

  it('omits and warns on a mechanism fromEnv cannot build (aws, oauthbearer, gssapi, ...)', () => {
    const { diagnostics, onDiagnostic } = collect();

    const config = fromEnv(
      { KAFKA_SASL_MECHANISM: 'aws', KAFKA_SASL_USERNAME: 'x', KAFKA_SASL_PASSWORD: 'y' },
      { onDiagnostic },
    );

    expect(config).not.toHaveProperty('sasl');
    expect(diagnostics).toHaveLength(1);
    expect(diagnostics[0]?.key).toBe('KAFKA_SASL_MECHANISM');
  });

  it('omits and warns when a username is set with no mechanism', () => {
    const { diagnostics, onDiagnostic } = collect();

    const config = fromEnv({ KAFKA_SASL_USERNAME: 'alice' }, { onDiagnostic });

    expect(config).not.toHaveProperty('sasl');
    expect(diagnostics).toHaveLength(1);
    expect(diagnostics[0]?.key).toBe('KAFKA_SASL_USERNAME');
  });

  it('omits and warns when a mechanism is set with no username or password', () => {
    const { diagnostics, onDiagnostic } = collect();

    const config = fromEnv({ KAFKA_SASL_MECHANISM: 'plain' }, { onDiagnostic });

    expect(config).not.toHaveProperty('sasl');
    expect(diagnostics).toHaveLength(1);
  });

  it('parses KAFKA_SSL as a boolean', () => {
    expect(fromEnv({ KAFKA_SSL: 'true' }).ssl).toBe(true);
    expect(fromEnv({ KAFKA_SSL: '0' }).ssl).toBe(false);
  });

  it('omits and warns on a non-boolean KAFKA_SSL', () => {
    const { diagnostics, onDiagnostic } = collect();

    const config = fromEnv({ KAFKA_SSL: 'maybe' }, { onDiagnostic });

    expect(config).not.toHaveProperty('ssl');
    expect(diagnostics).toHaveLength(1);
    expect(diagnostics[0]?.key).toBe('KAFKA_SSL');
  });

  describe('ssl file options', () => {
    let dir: string | undefined;

    afterEach(() => {
      if (dir != null) {
        rmSync(dir, { recursive: true, force: true });
        dir = undefined;
      }
    });

    function tempFile(content: string, filename: string): string {
      dir = mkdtempSync(join(tmpdir(), 'kafka-core-from-env-'));
      const path = join(dir, filename);
      writeFileSync(path, content);
      return path;
    }

    it('reads KAFKA_SSL_CA_FILE/_CERT_FILE/_KEY_FILE into an object, ignoring the plain KAFKA_SSL boolean', () => {
      const caFile = tempFile('ca-contents', 'ca.pem');
      const certFile = tempFile('cert-contents', 'cert.pem');
      const keyFile = tempFile('key-contents', 'key.pem');

      const config = fromEnv({
        KAFKA_SSL_CA_FILE: caFile,
        KAFKA_SSL_CERT_FILE: certFile,
        KAFKA_SSL_KEY_FILE: keyFile,
        KAFKA_SSL_REJECT_UNAUTHORIZED: 'false',
      });

      expect(config.ssl).toEqual({
        ca: 'ca-contents',
        cert: 'cert-contents',
        key: 'key-contents',
        rejectUnauthorized: false,
      });
    });

    it('warns and omits just the one field when a referenced ssl file does not exist', () => {
      const { diagnostics, onDiagnostic } = collect();

      const config = fromEnv({ KAFKA_SSL_CA_FILE: '/does/not/exist.pem' }, { onDiagnostic });

      expect(config.ssl).toEqual({ ca: undefined, cert: undefined, key: undefined, rejectUnauthorized: undefined });
      expect(diagnostics).toHaveLength(1);
      expect(diagnostics[0]?.key).toBe('KAFKA_SSL_CA_FILE');
    });
  });

  it('supports a custom prefix', () => {
    const config = fromEnv({ MYAPP_BROKERS: 'a:1', KAFKA_BROKERS: 'b:1' }, { prefix: 'MYAPP_' });

    expect(config.brokers).toEqual(['a:1']);
  });

  it('defaultOnFromEnvDiagnostic writes a warning to stderr, prefixed [kafka]', () => {
    const spy = vi.spyOn(process.stderr, 'write').mockImplementation(() => true);

    defaultOnFromEnvDiagnostic({ code: 'config.env-invalid', level: 'warn', key: 'KAFKA_SSL', message: 'boom' });

    expect(spy).toHaveBeenCalledWith('[kafka] boom\n');
    spy.mockRestore();
  });
});
