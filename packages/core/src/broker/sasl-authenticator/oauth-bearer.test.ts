import { describe, expect, it, vi } from 'vitest';
import { createLogger, LOG_LEVELS } from '../../loggers/index';
import { oauthBearerAuthenticatorProvider } from './oauth-bearer';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

describe('broker/sasl-authenticator/oauth-bearer', () => {
  it('throws for a missing oauthBearerProvider', async () => {
    const oauthBearer = oauthBearerAuthenticatorProvider({})({
      host: '',
      port: 0,
      logger: silentLogger,
      saslAuthenticate: vi.fn(),
    });

    await expect(oauthBearer.authenticate()).rejects.toThrow('Missing OAuth bearer token provider');
  });

  it('throws for an invalid OAuth bearer token', async () => {
    const oauthBearer = oauthBearerAuthenticatorProvider({
      oauthBearerProvider: async () => ({}) as never,
    })({ host: '', port: 0, logger: silentLogger, saslAuthenticate: vi.fn() });

    await expect(oauthBearer.authenticate()).rejects.toThrow('Invalid OAuth bearer token');
  });

  it('sends the OAUTHBEARER request through saslAuthenticate on success', async () => {
    const saslAuthenticate = vi.fn().mockResolvedValue(undefined);
    const oauthBearer = oauthBearerAuthenticatorProvider({
      oauthBearerProvider: async () => ({ value: 'my-token' }),
    })({ host: 'broker', port: 9092, logger: silentLogger, saslAuthenticate });

    await oauthBearer.authenticate();
    expect(saslAuthenticate).toHaveBeenCalledOnce();
    const [args] = saslAuthenticate.mock.calls[0]!;
    expect(args.request).toBeDefined();
    expect(args.response).toBeDefined();
  });
});
