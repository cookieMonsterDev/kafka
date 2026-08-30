import { describe, expect, it } from 'vitest';
import { redactKafkaConfig } from './redact';

describe('redactKafkaConfig', () => {
  it('redacts every allow-listed secret field and leaves everything else untouched', () => {
    const config = {
      brokers: ['a:9092'],
      clientId: 'app',
      sasl: {
        mechanism: 'scram-sha-256',
        username: 'alice',
        password: 'super-secret',
        tokenHmac: 'hmac-secret',
      },
      ssl: {
        rejectUnauthorized: true,
        key: 'private-key-pem',
        pfx: 'pfx-bytes',
        passphrase: 'ssl-secret',
        ca: 'public-ca-pem',
      },
    };

    const redacted = redactKafkaConfig(config) as typeof config;

    expect(redacted.sasl.password).toBe('[REDACTED]');
    expect(redacted.sasl.tokenHmac).toBe('[REDACTED]');
    expect(redacted.sasl.username).toBe('alice');
    expect(redacted.sasl.mechanism).toBe('scram-sha-256');
    expect(redacted.ssl.key).toBe('[REDACTED]');
    expect(redacted.ssl.pfx).toBe('[REDACTED]');
    expect(redacted.ssl.passphrase).toBe('[REDACTED]');
    expect(redacted.ssl.ca).toBe('public-ca-pem');
    expect(redacted.ssl.rejectUnauthorized).toBe(true);
    expect(redacted.brokers).toEqual(['a:9092']);
    expect(redacted.clientId).toBe('app');

    const serialized = JSON.stringify(redacted);
    expect(serialized).not.toContain('super-secret');
    expect(serialized).not.toContain('hmac-secret');
    expect(serialized).not.toContain('private-key-pem');
    expect(serialized).not.toContain('pfx-bytes');
    expect(serialized).not.toContain('ssl-secret');
  });

  it('redacts a sasl/ssl section nested under a KafkaFileConfig client key too', () => {
    const fileConfig = { client: { sasl: { mechanism: 'plain', username: 'a', password: 'nested-secret' } } };

    const redacted = redactKafkaConfig(fileConfig) as typeof fileConfig;

    expect(redacted.client.sasl.password).toBe('[REDACTED]');
    expect(JSON.stringify(redacted)).not.toContain('nested-secret');
  });

  it('an aws sasl mechanism redacts secretAccessKey and sessionToken', () => {
    const config = {
      sasl: {
        mechanism: 'aws',
        authorizationIdentity: 'id',
        accessKeyId: 'AKIA...',
        secretAccessKey: 'aws-secret',
        sessionToken: 'aws-session-secret',
      },
    };

    const redacted = redactKafkaConfig(config) as typeof config;

    expect(redacted.sasl.secretAccessKey).toBe('[REDACTED]');
    expect(redacted.sasl.sessionToken).toBe('[REDACTED]');
    expect(redacted.sasl.accessKeyId).toBe('AKIA...');
  });

  it('turns a function into [Function]', () => {
    const redacted = redactKafkaConfig({ socketFactory: () => undefined }) as { socketFactory: unknown };

    expect(redacted.socketFactory).toBe('[Function]');
  });

  it('turns a Buffer into [Buffer N bytes]', () => {
    const redacted = redactKafkaConfig({ ssl: { key: Buffer.from('abc') } }) as { ssl: { key: unknown } };

    // `key` is inside the ssl secret allow-list, so it is `[REDACTED]` even though it is a Buffer —
    // the allow-list takes priority over the value's type.
    expect(redacted.ssl.key).toBe('[REDACTED]');

    const bufferElsewhere = redactKafkaConfig({ notASecret: Buffer.from('abcde') }) as { notASecret: unknown };
    expect(bufferElsewhere.notASecret).toBe('[Buffer 5 bytes]');
  });

  it('does not throw on a circular reference and marks it [Circular]', () => {
    const config: Record<string, unknown> = { clientId: 'app' };
    config.self = config;

    let redacted: unknown;
    expect(() => {
      redacted = redactKafkaConfig(config);
    }).not.toThrow();
    expect((redacted as { self: unknown }).self).toBe('[Circular]');
    expect(() => JSON.stringify(redacted)).not.toThrow();
  });

  it('does not throw when a property getter itself throws, reporting it as [Unreadable]', () => {
    const config: Record<string, unknown> = {
      clientId: 'app',
      get sasl(): never {
        throw new Error('boom');
      },
    };

    let redacted: unknown;
    expect(() => {
      redacted = redactKafkaConfig(config);
    }).not.toThrow();
    expect((redacted as { sasl: unknown }).sasl).toBe('[Unreadable]');
    expect(() => JSON.stringify(redacted)).not.toThrow();
  });

  it('redacts every secret field even when the same object is aliased as both sasl and ssl', () => {
    const shared = { password: 'sasl-secret', key: 'ssl-secret' };
    const config = { sasl: shared, ssl: shared };

    const redacted = redactKafkaConfig(config) as { sasl: typeof shared; ssl: typeof shared };

    expect(redacted.sasl.password).toBe('[REDACTED]');
    expect(redacted.sasl.key).toBe('[REDACTED]');
    expect(redacted.ssl.password).toBe('[REDACTED]');
    expect(redacted.ssl.key).toBe('[REDACTED]');
    const serialized = JSON.stringify(redacted);
    expect(serialized).not.toContain('sasl-secret');
    expect(serialized).not.toContain('ssl-secret');
  });

  it('redacts secret fields inside an array of sasl-like objects', () => {
    const redacted = redactKafkaConfig({ sasl: [{ password: 'secret1' }, { password: 'secret2' }] }) as {
      sasl: { password: string }[];
    };

    expect(redacted.sasl[0]?.password).toBe('[REDACTED]');
    expect(redacted.sasl[1]?.password).toBe('[REDACTED]');
  });

  it('matches sasl/ssl case-insensitively', () => {
    const redacted = redactKafkaConfig({ SASL: { password: 'secret3' } }) as { SASL: { password: string } };

    expect(redacted.SASL.password).toBe('[REDACTED]');
  });

  it('JSON.stringify(redactKafkaConfig(x)) is safe for any input', () => {
    expect(() => JSON.stringify(redactKafkaConfig(null))).not.toThrow();
    expect(() => JSON.stringify(redactKafkaConfig(undefined))).not.toThrow();
    expect(() => JSON.stringify(redactKafkaConfig('a string'))).not.toThrow();
    expect(() => JSON.stringify(redactKafkaConfig(42))).not.toThrow();
    expect(() => JSON.stringify(redactKafkaConfig([1, 2, { sasl: { password: 'x' } }]))).not.toThrow();
  });
});
