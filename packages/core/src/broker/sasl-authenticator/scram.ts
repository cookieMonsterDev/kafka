import crypto from 'node:crypto';
import { KafkaNonRetriableError, KafkaSASLAuthenticationError } from '../../errors';
import type { Logger } from '../../loggers/index';
import { scramFinalMessageRequest, scramFirstMessageRequest, scramResponse } from '../../protocol/sasl/scram';
import type { ScramServerMessage } from '../../protocol/sasl/scram';

const GS2_HEADER = 'n,,';

const EQUAL_SIGN_REGEX = /=/g;
const COMMA_SIGN_REGEX = /,/g;

const URLSAFE_BASE64_PLUS_REGEX = /\+/g;
const URLSAFE_BASE64_SLASH_REGEX = /\//g;
const URLSAFE_BASE64_TRAILING_EQUAL_REGEX = /=+$/;

const HMAC_CLIENT_KEY = 'Client Key';
const HMAC_SERVER_KEY = 'Server Key';

export interface DigestDefinition {
  length: number;
  type: 'sha256' | 'sha512';
  minIterations: number;
}

export const DIGESTS = Object.freeze({
  SHA256: Object.freeze({ length: 32, type: 'sha256', minIterations: 4096 }),
  SHA512: Object.freeze({ length: 64, type: 'sha512', minIterations: 4096 }),
});

/**
 * Kafka's `ScramLoginModule` tokenauth JAAS option. When true, the client-first
 * SCRAM message includes the `tokenauth=true` extension so the broker looks up a
 * delegation token instead of a stored SCRAM user.
 *
 * @see https://kafka.apache.org/43/security/authentication-using-sasl/
 */
export const TOKEN_AUTH_EXTENSION = 'tokenauth=true';

/** Public SASL SCRAM fields before mapping token credentials onto username/password. */
export interface ScramSaslInput {
  username?: string;
  password?: string;
  tokenId?: string;
  tokenHmac?: Buffer | string;
}

export interface ScramSaslConfig {
  username: string;
  password: string;
  /**
   * When true, append `tokenauth=true` to the client-first message (KIP-48).
   * Username is the token id; password is the token HMAC as a base64 string.
   */
  tokenAuth?: boolean;
}

function hasTokenHmac(hmac: unknown): hmac is Buffer | string {
  if (typeof hmac === 'string') return hmac.length > 0;
  return Buffer.isBuffer(hmac) && hmac.length > 0;
}

function encodeTokenHmac(hmac: Buffer | string): string {
  return Buffer.isBuffer(hmac) ? hmac.toString('base64') : hmac;
}

/**
 * Map first-class delegation-token fields onto SCRAM username/password.
 * `tokenId` is the username and `tokenHmac` is the password (Buffer values are
 * encoded as standard base64, matching Java `DelegationToken#hmacAsBase64String`).
 */
export function resolveScramSaslConfig(sasl: ScramSaslInput): ScramSaslConfig {
  const hasTokenId = sasl.tokenId != null && sasl.tokenId.length > 0;
  const hmacPresent = hasTokenHmac(sasl.tokenHmac);

  if (hasTokenId || hmacPresent) {
    if (sasl.tokenId == null || sasl.tokenId.length === 0 || !hasTokenHmac(sasl.tokenHmac)) {
      throw new KafkaSASLAuthenticationError('SASL SCRAM: token authentication requires both tokenId and tokenHmac');
    }

    return {
      username: sasl.tokenId,
      password: encodeTokenHmac(sasl.tokenHmac),
      tokenAuth: true,
    };
  }

  return { username: sasl.username as string, password: sasl.password as string };
}

export type SaslAuthenticateFn = <Decoded, ParseResult = Decoded>(args: {
  request: { encode(): Buffer | Promise<Buffer> };
  response?: { decode(rawResponse: Buffer): Promise<Decoded>; parse(data: Decoded): Promise<ParseResult> };
}) => Promise<ParseResult | undefined>;

const encode64 = (str: string | Buffer): string => Buffer.from(str).toString('base64');

/** https://tools.ietf.org/html/rfc5802 */
export class SCRAM {
  currentNonce: string;

  #sasl: ScramSaslConfig;
  #host: string;
  #port: number;
  #logger: Logger;
  #saslAuthenticate: SaslAuthenticateFn;
  #digestDefinition: DigestDefinition;
  #prefix: string;

  /**
   * The characters ',' or '=' in usernames are sent as '=2C' and '=3D' respectively (RFC 5802
   * section 5.1). If the server receives a username containing '=' not followed by either '2C' or
   * '3D', authentication MUST fail.
   */
  static sanitizeString(str: string): string {
    return str.replace(EQUAL_SIGN_REGEX, '=3D').replace(COMMA_SIGN_REGEX, '=2C');
  }

  /** A nonce ensures old communications cannot be reused in replay attacks. */
  static nonce(): string {
    return crypto
      .randomBytes(16)
      .toString('base64')
      .replace(URLSAFE_BASE64_PLUS_REGEX, '-') // make it url safe
      .replace(URLSAFE_BASE64_SLASH_REGEX, '_')
      .replace(URLSAFE_BASE64_TRAILING_EQUAL_REGEX, '');
  }

  /**
   * Hi() is, essentially, PBKDF2 [RFC2898] with HMAC() as the pseudorandom function (PRF) and with
   * dkLen == output length of HMAC() == output length of H().
   */
  static hi(password: string, salt: Buffer, iterations: number, digestDefinition: DigestDefinition): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      crypto.pbkdf2(password, salt, iterations, digestDefinition.length, digestDefinition.type, (err, derivedKey) =>
        err ? reject(err) : resolve(derivedKey),
      );
    });
  }

  /** Exclusive-or two equal-length octet strings. */
  static xor(left: Buffer, right: Buffer): Buffer {
    const length = Buffer.byteLength(left);

    if (length !== Buffer.byteLength(right)) {
      throw new KafkaNonRetriableError('Buffers must be of the same length');
    }

    const result = new Array<number>(length);
    for (let i = 0; i < length; i++) {
      result[i] = left[i]! ^ right[i]!;
    }

    return Buffer.from(result);
  }

  constructor(
    sasl: ScramSaslConfig,
    host: string,
    port: number,
    logger: Logger,
    saslAuthenticate: SaslAuthenticateFn,
    digestDefinition: DigestDefinition,
  ) {
    this.#sasl = sasl;
    this.#host = host;
    this.#port = port;
    this.#logger = logger;
    this.#saslAuthenticate = saslAuthenticate;
    this.#digestDefinition = digestDefinition;
    this.#prefix = `SASL SCRAM ${digestDefinition.type.toUpperCase()} authentication`;
    this.currentNonce = SCRAM.nonce();
  }

  async authenticate(): Promise<void> {
    const broker = `${this.#host}:${this.#port}`;

    if (this.#sasl.username == null || this.#sasl.password == null) {
      throw new KafkaSASLAuthenticationError(`${this.#prefix}: Invalid username or password`);
    }

    try {
      this.#logger.debug('Exchanging first client message', { broker });
      const clientMessageResponse = await this.sendClientFirstMessage();

      this.#logger.debug('Sending final message', { broker });
      const finalResponse = await this.sendClientFinalMessage(clientMessageResponse);

      if (finalResponse.e) {
        throw new Error(finalResponse.e);
      }

      const serverKey = await this.serverKey(clientMessageResponse);
      const serverSignature = this.serverSignature(serverKey, clientMessageResponse);

      if (finalResponse.v !== serverSignature) {
        throw new Error('Invalid server signature in server final message');
      }

      this.#logger.debug(`${this.#prefix} successful`, { broker });
    } catch (e) {
      const error = new KafkaSASLAuthenticationError(`${this.#prefix} failed: ${(e as Error).message}`);
      this.#logger.error(error.message, { broker });
      throw error;
    }
  }

  private async sendClientFirstMessage(): Promise<ScramServerMessage> {
    const clientFirstMessage = `${GS2_HEADER}${this.firstMessageBare()}`;
    const request = scramFirstMessageRequest({ clientFirstMessage });

    const response = await this.#saslAuthenticate({ request, response: scramResponse });
    if (!response) throw new KafkaNonRetriableError('SCRAM: broker did not respond to the first client message');
    return response;
  }

  private async sendClientFinalMessage(clientMessageResponse: ScramServerMessage): Promise<ScramServerMessage> {
    const iterations = parseInt(clientMessageResponse.i ?? '', 10);
    const { minIterations } = this.#digestDefinition;

    if (!clientMessageResponse.r?.startsWith(this.currentNonce)) {
      throw new KafkaSASLAuthenticationError(
        `${this.#prefix} failed: Invalid server nonce, it does not start with the client nonce`,
      );
    }

    if (iterations < minIterations) {
      throw new KafkaSASLAuthenticationError(
        `${this.#prefix} failed: Requested iterations ${iterations} is less than the minimum ${minIterations}`,
      );
    }

    const finalMessageWithoutProof = this.finalMessageWithoutProof(clientMessageResponse);
    const clientProof = await this.clientProof(clientMessageResponse);
    const finalMessage = `${finalMessageWithoutProof},p=${clientProof}`;
    const request = scramFinalMessageRequest({ finalMessage });

    const response = await this.#saslAuthenticate({ request, response: scramResponse });
    if (!response) throw new KafkaNonRetriableError('SCRAM: broker did not respond to the final client message');
    return response;
  }

  private async clientProof(clientMessageResponse: ScramServerMessage): Promise<string> {
    const clientKey = await this.clientKey(clientMessageResponse);
    const storedKey = this.H(clientKey);
    const clientSignature = this.clientSignature(storedKey, clientMessageResponse);
    return encode64(SCRAM.xor(clientKey, clientSignature));
  }

  private async clientKey(clientMessageResponse: ScramServerMessage): Promise<Buffer> {
    const saltedPassword = await this.saltPassword(clientMessageResponse);
    return this.HMAC(saltedPassword, HMAC_CLIENT_KEY);
  }

  private async serverKey(clientMessageResponse: ScramServerMessage): Promise<Buffer> {
    const saltedPassword = await this.saltPassword(clientMessageResponse);
    return this.HMAC(saltedPassword, HMAC_SERVER_KEY);
  }

  private clientSignature(storedKey: Buffer, clientMessageResponse: ScramServerMessage): Buffer {
    return this.HMAC(storedKey, this.authMessage(clientMessageResponse));
  }

  private serverSignature(serverKey: Buffer, clientMessageResponse: ScramServerMessage): string {
    return encode64(this.HMAC(serverKey, this.authMessage(clientMessageResponse)));
  }

  private authMessage(clientMessageResponse: ScramServerMessage): string {
    return [
      this.firstMessageBare(),
      clientMessageResponse.original,
      this.finalMessageWithoutProof(clientMessageResponse),
    ].join(',');
  }

  private async saltPassword(clientMessageResponse: ScramServerMessage): Promise<Buffer> {
    const salt = Buffer.from(clientMessageResponse.s ?? '', 'base64');
    const iterations = parseInt(clientMessageResponse.i ?? '', 10);
    return SCRAM.hi(this.encodedPassword(), salt, iterations, this.#digestDefinition);
  }

  private firstMessageBare(): string {
    const bare = `n=${this.encodedUsername()},r=${this.currentNonce}`;
    return this.#sasl.tokenAuth ? `${bare},${TOKEN_AUTH_EXTENSION}` : bare;
  }

  private finalMessageWithoutProof(clientMessageResponse: ScramServerMessage): string {
    return `c=${encode64(GS2_HEADER)},r=${clientMessageResponse.r}`;
  }

  private encodedUsername(): string {
    return SCRAM.sanitizeString(this.#sasl.username);
  }

  private encodedPassword(): string {
    return this.#sasl.password;
  }

  H(data: Buffer): Buffer {
    return crypto.createHash(this.#digestDefinition.type).update(data).digest();
  }

  HMAC(key: Buffer, data: string): Buffer {
    return crypto.createHmac(this.#digestDefinition.type, key).update(data).digest();
  }
}
