import { KafkaJSSASLAuthenticationError } from '../../errors.js';
import type { AuthenticationProviderArgs, SaslAuthenticationProvider } from '../../network/connection.js';
import { awsIamRequest, awsIamResponse } from '../../protocol/sasl/aws-iam.js';
import type { AwsIamSaslConfig } from '../../protocol/sasl/aws-iam.js';

export function awsIamAuthenticatorProvider(
  sasl: AwsIamSaslConfig,
): (args: AuthenticationProviderArgs) => SaslAuthenticationProvider {
  return ({ host, port, logger, saslAuthenticate }) => ({
    authenticate: async () => {
      if (!sasl.authorizationIdentity) {
        throw new KafkaJSSASLAuthenticationError('SASL AWS-IAM: Missing authorizationIdentity');
      }
      if (!sasl.accessKeyId) {
        throw new KafkaJSSASLAuthenticationError('SASL AWS-IAM: Missing accessKeyId');
      }
      if (!sasl.secretAccessKey) {
        throw new KafkaJSSASLAuthenticationError('SASL AWS-IAM: Missing secretAccessKey');
      }
      if (!sasl.sessionToken) {
        sasl.sessionToken = '';
      }

      const broker = `${host}:${port}`;

      try {
        logger.debug('Authenticate with SASL AWS-IAM', { broker });
        await saslAuthenticate({ request: awsIamRequest(sasl), response: awsIamResponse });
        logger.debug('SASL AWS-IAM authentication successful', { broker });
      } catch (e) {
        const error = new KafkaJSSASLAuthenticationError(`SASL AWS-IAM authentication failed: ${(e as Error).message}`);
        logger.error(error.message, { broker });
        throw error;
      }
    },
  });
}
