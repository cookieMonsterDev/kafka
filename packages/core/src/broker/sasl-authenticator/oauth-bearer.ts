/**
 * The sasl object must include a property named oauthBearerProvider, an async function that is
 * used to return the OAuth bearer token.
 *
 * The OAuth bearer token must be an object with properties value and (optionally) extensions,
 * that will be sent during the SASL/OAUTHBEARER request.
 *
 * The implementation of the oauthBearerProvider must take care that tokens are reused and
 * refreshed when appropriate.
 */
import { KafkaJSSASLAuthenticationError } from '../../errors.js';
import type { AuthenticationProviderArgs, SaslAuthenticationProvider } from '../../network/connection.js';
import { oauthBearerRequest } from '../../protocol/sasl/oauth-bearer.js';
import type { OauthBearerSaslConfig, OauthBearerToken } from '../../protocol/sasl/oauth-bearer.js';

export interface OauthBearerConfig extends OauthBearerSaslConfig {
  oauthBearerProvider?: () => Promise<OauthBearerToken>;
}

export function oauthBearerAuthenticatorProvider(
  sasl: OauthBearerConfig,
): (args: AuthenticationProviderArgs) => SaslAuthenticationProvider {
  return ({ host, port, logger, saslAuthenticate }) => ({
    authenticate: async () => {
      const { oauthBearerProvider } = sasl;

      if (oauthBearerProvider == null) {
        throw new KafkaJSSASLAuthenticationError('SASL OAUTHBEARER: Missing OAuth bearer token provider');
      }

      const oauthBearerToken = await oauthBearerProvider();

      if (oauthBearerToken.value == null) {
        throw new KafkaJSSASLAuthenticationError('SASL OAUTHBEARER: Invalid OAuth bearer token');
      }

      const broker = `${host}:${port}`;

      try {
        logger.debug('Authenticate with SASL OAUTHBEARER', { broker });
        await saslAuthenticate({ request: await oauthBearerRequest(sasl, oauthBearerToken) });
        logger.debug('SASL OAUTHBEARER authentication successful', { broker });
      } catch (e) {
        const error = new KafkaJSSASLAuthenticationError(
          `SASL OAUTHBEARER authentication failed: ${(e as Error).message}`,
        );
        logger.error(error.message, { broker });
        throw error;
      }
    },
  });
}
