import { beforeEach, describe, expect, it, vi } from 'vitest';
import { createLogger, LOG_LEVELS } from '../../loggers/index';
import { Decoder } from '../../protocol/decoder';
import { scram256AuthenticatorProvider } from './scram256';
import { DIGESTS, resolveScramSaslConfig, SCRAM } from './scram';
import type { ScramServerMessage } from '../../protocol/sasl/scram';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

/** Reaches past the class's `private` (compile-time-only) crypto helpers for known-answer testing. */
interface ScramInternals {
  saltPassword(clientMessageResponse: ScramServerMessage): Promise<Buffer>;
  clientKey(clientMessageResponse: ScramServerMessage): Promise<Buffer>;
  H(data: Buffer): Buffer;
  sendClientFirstMessage(): Promise<ScramServerMessage>;
  sendClientFinalMessage(clientMessageResponse: ScramServerMessage): Promise<ScramServerMessage>;
}

const internals = (scram: SCRAM): ScramInternals => scram as unknown as ScramInternals;

describe('broker/sasl-authenticator/SCRAM', () => {
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

  describe('SCRAM SHA-256', () => {
    const saslAuthenticate = vi.fn();
    const createScram = (password = 'pencil') =>
      new SCRAM({ username: 'user', password }, 'host', 9094, silentLogger, saslAuthenticate, DIGESTS.SHA256);

    beforeEach(() => {
      saslAuthenticate.mockClear();
    });

    it('saltPassword derives the expected key via PBKDF2', async () => {
      const scram = createScram('password');
      const clientMessageResponse: ScramServerMessage = {
        original: '',
        s: 'enBxNzV4aGphMjJmbnZ0ejF5M2o4Y3JjdA==',
        i: '4096',
      };
      const saltedPassword = await internals(scram).saltPassword(clientMessageResponse);
      expect(saltedPassword.toString('hex')).toBe('72c2aaf3a8fd5732b83c5bd9fbf8d0c6e851d8d18d56fbb4e73813acf267009e');
    });

    it('clientKey derives an HMAC of the salted password', async () => {
      const scram = createScram('password');
      const clientMessageResponse: ScramServerMessage = {
        original: '',
        s: 'enBxNzV4aGphMjJmbnZ0ejF5M2o4Y3JjdA==',
        i: '4096',
      };
      const clientKey = await internals(scram).clientKey(clientMessageResponse);
      expect(clientKey.toString('hex')).toBe('21819e176123554b9cec1dc1799b25ba112ae3c1d80e2b693476d28d99a15193');
    });

    it('H hashes the client key into the stored key', async () => {
      const scram = createScram('password');
      const clientMessageResponse: ScramServerMessage = {
        original: '',
        s: 'enBxNzV4aGphMjJmbnZ0ejF5M2o4Y3JjdA==',
        i: '4096',
      };
      const clientKey = await internals(scram).clientKey(clientMessageResponse);
      const storedKey = internals(scram).H(clientKey);
      expect(storedKey.toString('hex')).toBe('228713ebcc6a14f44503e9a0ecfe01d9e6b88adb39b890ade8b222fa4c323fd9');
    });

    describe('first message', () => {
      it('encodes the GS2 header, username, and client nonce', async () => {
        const scram = createScram();
        scram.currentNonce = 'rOprNGfwEbeRWgbNEkqO';
        saslAuthenticate.mockResolvedValueOnce({ original: '' });

        await internals(scram).sendClientFirstMessage();

        expect(saslAuthenticate).toHaveBeenCalledWith(
          expect.objectContaining({ request: expect.any(Object), response: expect.any(Object) }),
        );
        const { request } = saslAuthenticate.mock.calls[0]![0] as { request: { encode(): Promise<Buffer> } };
        const buffer = await request.encode();
        const decoder = new Decoder(buffer);
        expect(decoder.readBytes()!.toString()).toBe(`n,,n=user,r=${scram.currentNonce}`);
      });

      it('sanitizes a comma in the username', async () => {
        const scram = new SCRAM(
          { username: 'bob,', password: 'password' },
          'host',
          9094,
          silentLogger,
          saslAuthenticate,
          DIGESTS.SHA256,
        );
        saslAuthenticate.mockResolvedValueOnce({ original: '' });

        await internals(scram).sendClientFirstMessage();

        const { request } = saslAuthenticate.mock.calls[0]![0] as { request: { encode(): Promise<Buffer> } };
        const buffer = await request.encode();
        const decoder = new Decoder(buffer);
        expect(decoder.readBytes()!.toString()).toBe(`n,,n=bob=2C,r=${scram.currentNonce}`);
      });

      it('sanitizes an equals sign in the username', async () => {
        const scram = new SCRAM(
          { username: 'bob=', password: 'password' },
          'host',
          9094,
          silentLogger,
          saslAuthenticate,
          DIGESTS.SHA256,
        );
        saslAuthenticate.mockResolvedValueOnce({ original: '' });

        await internals(scram).sendClientFirstMessage();

        const { request } = saslAuthenticate.mock.calls[0]![0] as { request: { encode(): Promise<Buffer> } };
        const buffer = await request.encode();
        const decoder = new Decoder(buffer);
        expect(decoder.readBytes()!.toString()).toBe(`n,,n=bob=3D,r=${scram.currentNonce}`);
      });
    });

    describe('delegation token', () => {
      it('maps tokenId and a Buffer hmac onto username, base64 password, and tokenAuth', () => {
        const hmac = Buffer.from('hmac-bytes');
        expect(resolveScramSaslConfig({ tokenId: 'tokenID123', tokenHmac: hmac })).toEqual({
          username: 'tokenID123',
          password: hmac.toString('base64'),
          tokenAuth: true,
        });
      });

      it('uses a string hmac as the SCRAM password without re-encoding', () => {
        expect(
          resolveScramSaslConfig({
            tokenId: 'tokenID123',
            tokenHmac: 'lAYYSFmLs4bTjf+lTZ1LCHR/ZZFNA==',
          }),
        ).toEqual({
          username: 'tokenID123',
          password: 'lAYYSFmLs4bTjf+lTZ1LCHR/ZZFNA==',
          tokenAuth: true,
        });
      });

      it('leaves username/password SCRAM without tokenAuth', () => {
        expect(resolveScramSaslConfig({ username: 'alice', password: 'secret' })).toEqual({
          username: 'alice',
          password: 'secret',
        });
      });

      it('throws when tokenId is missing', () => {
        expect(() => resolveScramSaslConfig({ tokenHmac: 'abc' })).toThrow(
          'token authentication requires both tokenId and tokenHmac',
        );
      });

      it('throws when tokenHmac is missing', () => {
        expect(() => resolveScramSaslConfig({ tokenId: 'tokenID123' })).toThrow(
          'token authentication requires both tokenId and tokenHmac',
        );
      });

      it('throws when tokenHmac is an empty Buffer', () => {
        expect(() => resolveScramSaslConfig({ tokenId: 'tokenID123', tokenHmac: Buffer.alloc(0) })).toThrow(
          'token authentication requires both tokenId and tokenHmac',
        );
      });

      it('encodes tokenauth=true on the client-first message', async () => {
        const hmac = Buffer.from('hmac-bytes');
        const scram = new SCRAM(
          resolveScramSaslConfig({ tokenId: 'tokenID123', tokenHmac: hmac }),
          'host',
          9094,
          silentLogger,
          saslAuthenticate,
          DIGESTS.SHA256,
        );
        scram.currentNonce = 'rOprNGfwEbeRWgbNEkqO';
        saslAuthenticate.mockResolvedValueOnce({ original: '' });

        await internals(scram).sendClientFirstMessage();

        const { request } = saslAuthenticate.mock.calls[0]![0] as { request: { encode(): Promise<Buffer> } };
        const buffer = await request.encode();
        const decoder = new Decoder(buffer);
        expect(decoder.readBytes()!.toString()).toBe(`n,,n=tokenID123,r=${scram.currentNonce},tokenauth=true`);
      });

      it('sends the mapped token first message through the SCRAM provider', async () => {
        const hmac = Buffer.from('hmac-bytes');
        const authenticate = scram256AuthenticatorProvider({ tokenId: 'tokenID123', tokenHmac: hmac })({
          host: 'host',
          port: 9094,
          logger: silentLogger,
          saslAuthenticate,
        });
        saslAuthenticate.mockResolvedValueOnce({
          original: 'r=nonce,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096',
          r: 'nonce',
          s: 'W22ZaJ0SNY7soEsUEjb6gQ==',
          i: '4096',
        });

        await expect(authenticate.authenticate()).rejects.toThrow('Invalid server nonce');

        const { request } = saslAuthenticate.mock.calls[0]![0] as { request: { encode(): Promise<Buffer> } };
        const buffer = await request.encode();
        const decoder = new Decoder(buffer);
        const firstMessage = decoder.readBytes()!.toString();
        expect(firstMessage.startsWith('n,,n=tokenID123,r=')).toBe(true);
        expect(firstMessage.endsWith(',tokenauth=true')).toBe(true);
      });
    });

    describe('second message', () => {
      it('encodes the final message using RFC 5802 section 5 example data', async () => {
        const scram = createScram();
        scram.currentNonce = 'rOprNGfwEbeRWgbNEkqO';
        const clientMessageResponse: ScramServerMessage = {
          original: 'r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096',
          r: 'rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0',
          s: 'W22ZaJ0SNY7soEsUEjb6gQ==',
          i: '4096',
        };
        saslAuthenticate.mockResolvedValueOnce({ original: '' });

        await internals(scram).sendClientFinalMessage(clientMessageResponse);

        const { request } = saslAuthenticate.mock.calls[0]![0] as { request: { encode(): Promise<Buffer> } };
        const buffer = await request.encode();
        const decoder = new Decoder(buffer);
        expect(decoder.readBytes()!.toString()).toBe(
          'c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=',
        );
      });
    });
  });
});
