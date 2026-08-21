import { KafkaSASLAuthenticationError } from '../../errors';
import type { AuthenticationProviderArgs, SaslAuthenticationProvider } from '../../network/connection';
import {
  DEFAULT_GSSAPI_SERVICE_NAME,
  MAX_GSSAPI_ROUNDS,
  gssapiRequest,
  gssapiResponse,
} from '../../protocol/sasl/gssapi';
import type { GssapiSaslConfig } from '../../protocol/sasl/gssapi';
import { createKerberosGssProvider, loadKerberosModule, type LoadKerberos } from './kerberos-gss-provider';

export interface GssapiAuthenticatorDeps {
  loadKerberos?: LoadKerberos;
}

export function gssapiAuthenticatorProvider(
  sasl: GssapiSaslConfig,
  deps: GssapiAuthenticatorDeps = {},
): (args: AuthenticationProviderArgs) => SaslAuthenticationProvider {
  return ({ host, port, logger, saslAuthenticate }) => ({
    authenticate: async () => {
      const serviceName = sasl.serviceName ?? DEFAULT_GSSAPI_SERVICE_NAME;
      const provider = sasl.gssProvider ?? createKerberosGssProvider(sasl, deps.loadKerberos ?? loadKerberosModule);
      const broker = `${host}:${port}`;

      try {
        logger.debug('Authenticate with SASL GSSAPI', { broker, serviceName });

        let serverToken: Buffer | null = null;
        for (let round = 0; round < MAX_GSSAPI_ROUNDS; round++) {
          const step = await provider({
            serverToken,
            host,
            port,
            serviceName,
            principal: sasl.principal,
          });

          if (step.token.length === 0 && step.complete) {
            logger.debug('SASL GSSAPI authentication successful', { broker });
            return;
          }

          const response = await saslAuthenticate({
            request: gssapiRequest(step.token),
            response: gssapiResponse,
          });
          serverToken = response ?? Buffer.alloc(0);

          if (step.complete) {
            logger.debug('SASL GSSAPI authentication successful', { broker });
            return;
          }
        }

        throw new KafkaSASLAuthenticationError(`SASL GSSAPI: exceeded ${MAX_GSSAPI_ROUNDS} token-exchange rounds`);
      } catch (e) {
        if (e instanceof KafkaSASLAuthenticationError) {
          logger.error(e.message, { broker });
          throw e;
        }
        const error = new KafkaSASLAuthenticationError(`SASL GSSAPI authentication failed: ${(e as Error).message}`);
        logger.error(error.message, { broker });
        throw error;
      }
    },
  });
}
