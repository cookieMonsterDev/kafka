import { KafkaJSSASLAuthenticationError } from '../../errors.js';
import type { AuthenticationProviderArgs, SaslAuthenticationProvider } from '../../network/connection.js';
import { plainRequest, plainResponse } from '../../protocol/sasl/plain.js';
import type { PlainSaslConfig } from '../../protocol/sasl/plain.js';

export function plainAuthenticatorProvider(
  sasl: PlainSaslConfig,
): (args: AuthenticationProviderArgs) => SaslAuthenticationProvider {
  return ({ host, port, logger, saslAuthenticate }) => ({
    authenticate: async () => {
      if (sasl.username == null || sasl.password == null) {
        throw new KafkaJSSASLAuthenticationError('SASL Plain: Invalid username or password');
      }

      const broker = `${host}:${port}`;

      try {
        logger.debug('Authenticate with SASL PLAIN', { broker });
        await saslAuthenticate({ request: plainRequest(sasl), response: plainResponse });
        logger.debug('SASL PLAIN authentication successful', { broker });
      } catch (e) {
        const error = new KafkaJSSASLAuthenticationError(`SASL PLAIN authentication failed: ${(e as Error).message}`);
        logger.error(error.message, { broker });
        throw error;
      }
    },
  });
}
