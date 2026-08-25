import { describe, expect, it, vi } from 'vitest';
import { createLogger, LOG_LEVELS } from '../../loggers/index';
import { Decoder } from '../../protocol/decoder';
import { scram256AuthenticatorProvider } from './scram256';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

describe('broker/sasl-authenticator/scram256AuthenticatorProvider', () => {
  it('throws for a missing username', async () => {
    const scram = scram256AuthenticatorProvider({ username: undefined, password: 'pencil' })({
      host: '',
      port: 0,
      logger: silentLogger,
      saslAuthenticate: vi.fn(),
    });
    await expect(scram.authenticate()).rejects.toThrow('Invalid username or password');
  });

  it('throws for a missing password', async () => {
    const scram = scram256AuthenticatorProvider({ username: 'user', password: undefined })({
      host: '',
      port: 0,
      logger: silentLogger,
      saslAuthenticate: vi.fn(),
    });
    await expect(scram.authenticate()).rejects.toThrow('Invalid username or password');
  });

  it('sends a well-formed client-first-message and rejects a mismatched server nonce', async () => {
    const saslAuthenticate = vi.fn().mockResolvedValueOnce({
      original: 'r=servernonce,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096',
      r: 'servernonce',
      s: 'W22ZaJ0SNY7soEsUEjb6gQ==',
      i: '4096',
    });

    const scram = scram256AuthenticatorProvider({ username: 'user', password: 'pencil' })({
      host: 'host',
      port: 9092,
      logger: silentLogger,
      saslAuthenticate,
    });

    await expect(scram.authenticate()).rejects.toThrow('Invalid server nonce');

    const { request } = saslAuthenticate.mock.calls[0]![0] as { request: { encode(): Promise<Buffer> } };
    const buffer = await request.encode();
    const decoder = new Decoder(buffer);
    const firstMessage = decoder.readBytes()!.toString();
    expect(firstMessage.startsWith('n,,n=user,r=')).toBe(true);
  });
});
