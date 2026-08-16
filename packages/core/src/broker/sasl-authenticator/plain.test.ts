import { describe, expect, it, vi } from 'vitest';
import { createLogger, LOG_LEVELS } from '../../loggers/index.js';
import { plainAuthenticatorProvider } from './plain.js';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

describe('broker/sasl-authenticator/plain', () => {
  it('throws for a missing username', async () => {
    const plain = plainAuthenticatorProvider({ username: undefined as never, password: 'pw' })({
      host: '',
      port: 0,
      logger: silentLogger,
      saslAuthenticate: vi.fn(),
    });

    await expect(plain.authenticate()).rejects.toThrow('Invalid username or password');
  });

  it('throws for a missing password', async () => {
    const plain = plainAuthenticatorProvider({ username: 'user', password: undefined as never })({
      host: '',
      port: 0,
      logger: silentLogger,
      saslAuthenticate: vi.fn(),
    });

    await expect(plain.authenticate()).rejects.toThrow('Invalid username or password');
  });

  it('sends the PLAIN request through saslAuthenticate on success', async () => {
    const saslAuthenticate = vi.fn().mockResolvedValue(true);
    const plain = plainAuthenticatorProvider({ username: 'user', password: 'pw' })({
      host: 'broker',
      port: 9092,
      logger: silentLogger,
      saslAuthenticate,
    });

    await plain.authenticate();
    expect(saslAuthenticate).toHaveBeenCalledOnce();
  });
});
