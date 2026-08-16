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
import { KafkaSASLAuthenticationError } from '../../errors';
import type { AuthenticationProviderArgs, SaslAuthenticationProvider } from '../../network/connection';
import { oauthBearerRequest, oauthBearerResponse } from '../../protocol/sasl/oauth-bearer';
import type { OauthBearerSaslConfig, OauthBearerToken } from '../../protocol/sasl/oauth-bearer';

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
        throw new KafkaSASLAuthenticationError('SASL OAUTHBEARER: Missing OAuth bearer token provider');
      }

      const oauthBearerToken = await oauthBearerProvider();

      if (oauthBearerToken.value == null) {
        throw new KafkaSASLAuthenticationError('SASL OAUTHBEARER: Invalid OAuth bearer token');
      }

      const broker = `${host}:${port}`;

      try {
        logger.debug('Authenticate with SASL OAUTHBEARER', { broker });
        await saslAuthenticate({
          request: await oauthBearerRequest(sasl, oauthBearerToken),
          response: oauthBearerResponse,
        });
        logger.debug('SASL OAUTHBEARER authentication successful', { broker });
      } catch (e) {
        const error = new KafkaSASLAuthenticationError(
          `SASL OAUTHBEARER authentication failed: ${(e as Error).message}`,
        );
        logger.error(error.message, { broker });
        throw error;
      }
    },
  });
}
