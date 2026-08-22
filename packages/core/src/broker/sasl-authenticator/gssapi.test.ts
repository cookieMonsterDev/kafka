import { describe, expect, it, vi } from 'vitest';
import { KafkaSASLAuthenticationError } from '../../errors';
import { createLogger, LOG_LEVELS } from '../../loggers/index';
import type { AuthenticationProviderArgs } from '../../network/connection';
import { MAX_GSSAPI_ROUNDS } from '../../protocol/sasl/gssapi';
import { gssapiAuthenticatorProvider } from './gssapi';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

describe('broker/sasl-authenticator/gssapi', () => {
  it('throws when gssProvider is missing and kerberos cannot be loaded', async () => {
    const gssapi = gssapiAuthenticatorProvider(
      {},
      {
        loadKerberos: async () => {
          throw new KafkaSASLAuthenticationError(
            'SASL GSSAPI: provide sasl.gssProvider or install the optional `kerberos` package',
          );
        },
      },
    )({ host: 'broker', port: 9092, logger: silentLogger, saslAuthenticate: vi.fn() });

    await expect(gssapi.authenticate()).rejects.toThrow('provide sasl.gssProvider');
  });

  it('exchanges mock GSS tokens until the provider completes', async () => {
    const clientTokens = [Buffer.from('client-1'), Buffer.from('client-2'), Buffer.from('client-wrap')];
    const serverTokens = [Buffer.from('server-1'), Buffer.from('server-2'), Buffer.from('server-wrap')];
    const challenges: Array<Buffer | null> = [];
    let round = 0;

    const saslAuthenticate = vi.fn(async () => {
      const server = serverTokens[round];
      round += 1;
      return server;
    });

    const gssapi = gssapiAuthenticatorProvider({
      serviceName: 'kafka',
      principal: 'user@EXAMPLE.COM',
      gssProvider: async ({ serverToken, host, port, serviceName, principal }) => {
        challenges.push(serverToken);
        expect(host).toBe('broker');
        expect(port).toBe(9092);
        expect(serviceName).toBe('kafka');
        expect(principal).toBe('user@EXAMPLE.COM');
        return { token: clientTokens[challenges.length - 1]!, complete: challenges.length === 3 };
      },
    })({
      host: 'broker',
      port: 9092,
      logger: silentLogger,
      saslAuthenticate: saslAuthenticate as AuthenticationProviderArgs['saslAuthenticate'],
    });

    await gssapi.authenticate();
    expect(saslAuthenticate).toHaveBeenCalledTimes(3);
    expect(challenges).toEqual([null, serverTokens[0], serverTokens[1]]);
  });

  it('does not send a final empty token when the provider is already complete', async () => {
    const saslAuthenticate = vi.fn();
    const gssapi = gssapiAuthenticatorProvider({
      gssProvider: async () => ({ token: Buffer.alloc(0), complete: true }),
    })({ host: 'broker', port: 9092, logger: silentLogger, saslAuthenticate });

    await gssapi.authenticate();
    expect(saslAuthenticate).not.toHaveBeenCalled();
  });

  it('wraps provider failures as KafkaSASLAuthenticationError', async () => {
    const gssapi = gssapiAuthenticatorProvider({
      gssProvider: async () => {
        throw new Error('kinit failed');
      },
    })({ host: 'broker', port: 9092, logger: silentLogger, saslAuthenticate: vi.fn() });

    await expect(gssapi.authenticate()).rejects.toThrow('SASL GSSAPI authentication failed: kinit failed');
  });

  it('throws when the token exchange exceeds the round cap', async () => {
    const saslAuthenticate = vi.fn().mockResolvedValue(Buffer.from('more'));
    const gssapi = gssapiAuthenticatorProvider({
      gssProvider: async () => ({ token: Buffer.from('again'), complete: false }),
    })({ host: 'broker', port: 9092, logger: silentLogger, saslAuthenticate });

    await expect(gssapi.authenticate()).rejects.toThrow(`exceeded ${MAX_GSSAPI_ROUNDS} token-exchange rounds`);
    expect(saslAuthenticate).toHaveBeenCalledTimes(MAX_GSSAPI_ROUNDS);
  });
});
