import { KafkaSASLAuthenticationError } from '../../errors';
import type { AuthenticationProviderArgs, SaslAuthenticationProvider } from '../../network/connection';
import { plainRequest, plainResponse } from '../../protocol/sasl/plain';
import type { PlainSaslConfig } from '../../protocol/sasl/plain';

export function plainAuthenticatorProvider(
  sasl: PlainSaslConfig,
): (args: AuthenticationProviderArgs) => SaslAuthenticationProvider {
  return ({ host, port, logger, saslAuthenticate }) => ({
    authenticate: async () => {
      if (sasl.username == null || sasl.password == null) {
        throw new KafkaSASLAuthenticationError('SASL Plain: Invalid username or password');
      }

      const broker = `${host}:${port}`;

      try {
        logger.debug('Authenticate with SASL PLAIN', { broker });
        await saslAuthenticate({ request: plainRequest(sasl), response: plainResponse });
        logger.debug('SASL PLAIN authentication successful', { broker });
      } catch (e) {
        const error = new KafkaSASLAuthenticationError(`SASL PLAIN authentication failed: ${(e as Error).message}`);
        logger.error(error.message, { broker });
        throw error;
      }
    },
  });
}
