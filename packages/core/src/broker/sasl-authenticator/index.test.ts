import { describe, expect, it, vi } from 'vitest';
import { createLogger, LOG_LEVELS } from '../../loggers/index';
import { Connection } from '../../network/connection';
import type { SaslConfig } from '../../network/connection';
import { createDefaultSocketFactory } from '../../network/socket-factory';
import { API_KEYS } from '../../protocol/requests/api-keys';
import { createSaslAuthenticator, SASLAuthenticator } from './index';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

const HANDSHAKE_ONLY_VERSIONS = { [API_KEYS.SaslHandshake]: { maxVersion: 1 } };
const HANDSHAKE_AND_AUTHENTICATE_VERSIONS = {
  [API_KEYS.SaslHandshake]: { maxVersion: 1 },
  [API_KEYS.SaslAuthenticate]: { maxVersion: 1 },
};

function createConnection(sasl: SaslConfig): Connection {
  return new Connection({
    host: '127.0.0.1',
    port: 9092,
    logger: silentLogger,
    socketFactory: createDefaultSocketFactory(),
    requestTimeout: 1000,
    connectionTimeout: 1000,
    sasl,
  });
}

describe('broker/sasl-authenticator/SASLAuthenticator', () => {
  it('throws when the broker does not advertise the requested mechanism', async () => {
    const connection = createConnection({ mechanism: 'plain', username: 'user', password: 'pw' });
    vi.spyOn(connection, 'send').mockResolvedValueOnce({ errorCode: 0, enabledMechanisms: ['SCRAM-SHA-256'] });

    const authenticator = new SASLAuthenticator(connection, silentLogger, HANDSHAKE_AND_AUTHENTICATE_VERSIONS, true);
    await expect(authenticator.authenticate()).rejects.toThrow('mechanism is not supported by the server');
  });

  it('authenticates PLAIN via SaslAuthenticate and records the session lifetime', async () => {
    const connection = createConnection({ mechanism: 'plain', username: 'user', password: 'pw' });
    const sendSpy = vi.spyOn(connection, 'send');
    sendSpy.mockResolvedValueOnce({ errorCode: 0, enabledMechanisms: ['PLAIN'] });
    sendSpy.mockResolvedValueOnce({
      errorCode: 0,
      errorMessage: null,
      authBytes: Buffer.alloc(0),
      sessionLifetimeMs: 3_600_000n,
    });

    const authenticator = new SASLAuthenticator(connection, silentLogger, HANDSHAKE_AND_AUTHENTICATE_VERSIONS, true);
    await authenticator.authenticate();

    expect(authenticator.sessionLifetime).toBe(3_600_000n);
    expect(sendSpy).toHaveBeenCalledTimes(2);
  });

  it('falls back to sendAuthRequest when the broker predates SaslAuthenticate', async () => {
    const connection = createConnection({ mechanism: 'plain', username: 'user', password: 'pw' });
    vi.spyOn(connection, 'send').mockResolvedValueOnce({ errorCode: 0, enabledMechanisms: ['PLAIN'] });
    const sendAuthRequestSpy = vi.spyOn(connection, 'sendAuthRequest').mockResolvedValue(true);

    const authenticator = new SASLAuthenticator(connection, silentLogger, HANDSHAKE_ONLY_VERSIONS, false);
    await authenticator.authenticate();

    expect(sendAuthRequestSpy).toHaveBeenCalledOnce();
  });

  it('wires a custom authenticationProvider through untouched', async () => {
    const authenticate = vi.fn().mockResolvedValue(undefined);
    const authenticationProvider = vi.fn().mockReturnValue({ authenticate });
    const connection = createConnection({ mechanism: 'custom', authenticationProvider });
    vi.spyOn(connection, 'send').mockResolvedValueOnce({ errorCode: 0, enabledMechanisms: ['CUSTOM'] });

    const authenticator = new SASLAuthenticator(connection, silentLogger, HANDSHAKE_AND_AUTHENTICATE_VERSIONS, true);
    await authenticator.authenticate();

    expect(authenticationProvider).toHaveBeenCalledOnce();
    expect(authenticate).toHaveBeenCalledOnce();
  });

  it('throws for an unknown mechanism with no authenticationProvider', async () => {
    const connection = createConnection({ mechanism: 'unknown-mechanism' });
    vi.spyOn(connection, 'send').mockResolvedValueOnce({ errorCode: 0, enabledMechanisms: ['UNKNOWN-MECHANISM'] });

    const authenticator = new SASLAuthenticator(connection, silentLogger, HANDSHAKE_AND_AUTHENTICATE_VERSIONS, true);
    await expect(authenticator.authenticate()).rejects.toThrow('has no authentication provider configured');
  });

  it('createSaslAuthenticator builds a SASLAuthenticator instance', () => {
    const connection = createConnection({ mechanism: 'plain' });
    const authenticator = createSaslAuthenticator(connection, silentLogger, HANDSHAKE_ONLY_VERSIONS, null);
    expect(authenticator).toBeInstanceOf(SASLAuthenticator);
  });
});
