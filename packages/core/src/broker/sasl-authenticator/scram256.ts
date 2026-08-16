import type { AuthenticationProviderArgs, SaslAuthenticationProvider } from '../../network/connection.js';
import { DIGESTS, SCRAM } from './scram.js';
import type { ScramSaslConfig } from './scram.js';

export function scram256AuthenticatorProvider(
  sasl: ScramSaslConfig,
): (args: AuthenticationProviderArgs) => SaslAuthenticationProvider {
  return ({ host, port, logger, saslAuthenticate }) => {
    const scram = new SCRAM(sasl, host, port, logger, saslAuthenticate, DIGESTS.SHA256);
    return { authenticate: () => scram.authenticate() };
  };
}
