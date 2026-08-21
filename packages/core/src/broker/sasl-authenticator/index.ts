import { KafkaSASLAuthenticationError } from '../../errors';
import type { Logger } from '../../loggers/index';
import type {
  AuthenticationProviderArgs,
  Connection,
  CreateSaslAuthenticator,
  SaslAuthenticationProvider,
} from '../../network/connection';
import { asTypedSend } from '../../network/connection';
import { API_KEYS } from '../../protocol/requests/api-keys';
import type { BrokerVersions, ProtocolFactory } from '../../protocol/requests/index';
import { lookup } from '../../protocol/requests/index';
import { SaslAuthenticate } from '../../protocol/requests/sasl-authenticate/index';
import type { SaslAuthenticateOptions } from '../../protocol/requests/sasl-authenticate/index';
import { SaslHandshake } from '../../protocol/requests/sasl-handshake/index';
import type { SaslHandshakeOptions } from '../../protocol/requests/sasl-handshake/index';
import { awsIamAuthenticatorProvider } from './aws-iam';
import { gssapiAuthenticatorProvider } from './gssapi';
import { oauthBearerAuthenticatorProvider } from './oauth-bearer';
import { plainAuthenticatorProvider } from './plain';
import { scram256AuthenticatorProvider } from './scram256';
import { scram512AuthenticatorProvider } from './scram512';

interface SaslHandshakeResult {
  errorCode: number;
  enabledMechanisms: string[];
}

/** v0 lacks `sessionLifetimeMs` entirely; v1 adds it. Cast target covers both, defaulting below. */
interface SaslAuthenticateResult {
  errorCode: number;
  errorMessage: string | null;
  authBytes: Buffer;
  sessionLifetimeMs?: bigint;
}

type BuiltInAuthenticatorProvider = (sasl: never) => (args: AuthenticationProviderArgs) => SaslAuthenticationProvider;

const BUILT_IN_AUTHENTICATION_PROVIDERS: Readonly<Record<string, BuiltInAuthenticatorProvider>> = {
  AWS: awsIamAuthenticatorProvider,
  GSSAPI: gssapiAuthenticatorProvider,
  PLAIN: plainAuthenticatorProvider,
  OAUTHBEARER: oauthBearerAuthenticatorProvider,
  'SCRAM-SHA-256': scram256AuthenticatorProvider,
  'SCRAM-SHA-512': scram512AuthenticatorProvider,
};

/**
 * Negotiates and runs one SASL exchange over an already-open `Connection`: `SaslHandshake` to
 * confirm the broker supports the requested mechanism, then the mechanism's own provider (built-in
 * or user-supplied) driving the actual exchange via `SaslAuthenticate` (or, on brokers old enough
 * to lack it, `Connection#sendAuthRequest`'s raw byte exchange).
 */
export class SASLAuthenticator {
  sessionLifetime = 0n;

  #connection: Connection;
  #logger: Logger;
  #saslHandshake: ProtocolFactory<SaslHandshakeOptions>;
  #protocolAuthentication: ProtocolFactory<SaslAuthenticateOptions> | null;

  constructor(
    connection: Connection,
    logger: Logger,
    versions: BrokerVersions | null,
    supportAuthenticationProtocol: boolean | null,
  ) {
    this.#connection = connection;
    this.#logger = logger;

    const lookupRequest = lookup(versions ?? {});
    this.#saslHandshake = lookupRequest(API_KEYS.SaslHandshake, SaslHandshake);
    this.#protocolAuthentication = supportAuthenticationProtocol
      ? lookupRequest(API_KEYS.SaslAuthenticate, SaslAuthenticate)
      : null;
  }

  async authenticate(): Promise<void> {
    const sasl = this.#connection.sasl;
    if (!sasl) throw new KafkaSASLAuthenticationError('SASL is not configured on this connection');

    const mechanism = sasl.mechanism.toUpperCase();
    const handshake = await this.#connection.send(asTypedSend<SaslHandshakeResult>(this.#saslHandshake({ mechanism })));

    if (!handshake?.enabledMechanisms.includes(mechanism)) {
      throw new KafkaSASLAuthenticationError(`SASL ${mechanism} mechanism is not supported by the server`);
    }

    const saslAuthenticate = async <Decoded, ParseResult = Decoded>({
      request,
      response,
    }: {
      request: { encode(): Buffer | Promise<Buffer> };
      response?: { decode(rawResponse: Buffer): Promise<Decoded>; parse(data: Decoded): Promise<ParseResult> };
    }): Promise<ParseResult | undefined> => {
      if (this.#protocolAuthentication) {
        const requestAuthBytes = await request.encode();
        const authResponse = await this.#connection.send(
          asTypedSend<SaslAuthenticateResult>(this.#protocolAuthentication({ authBytes: requestAuthBytes })),
        );

        this.sessionLifetime = authResponse?.sessionLifetimeMs ?? 0n;

        if (!response || !authResponse) return undefined;

        const payloadDecoded = await response.decode(authResponse.authBytes);
        return response.parse(payloadDecoded);
      }

      return this.#connection.sendAuthRequest({ request, response });
    };

    if (!sasl.authenticationProvider && mechanism in BUILT_IN_AUTHENTICATION_PROVIDERS) {
      sasl.authenticationProvider = BUILT_IN_AUTHENTICATION_PROVIDERS[mechanism]!(sasl as never);
    }

    if (!sasl.authenticationProvider) {
      throw new KafkaSASLAuthenticationError(`SASL ${mechanism} has no authentication provider configured`);
    }

    await sasl
      .authenticationProvider({
        host: this.#connection.host,
        port: this.#connection.port,
        logger: this.#logger.namespace(`SaslAuthenticator-${mechanism}`),
        saslAuthenticate,
      })
      .authenticate();
  }
}

export const createSaslAuthenticator: CreateSaslAuthenticator = (
  connection,
  logger,
  versions,
  supportAuthenticationProtocol,
) => new SASLAuthenticator(connection, logger, versions, supportAuthenticationProtocol);
