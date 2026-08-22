import type { AuthenticationProviderArgs, SaslAuthenticationProvider } from '../../network/connection';
import { DIGESTS, resolveScramSaslConfig, SCRAM } from './scram';
import type { ScramSaslInput } from './scram';

export function scram512AuthenticatorProvider(
  sasl: ScramSaslInput,
): (args: AuthenticationProviderArgs) => SaslAuthenticationProvider {
  return ({ host, port, logger, saslAuthenticate }) => {
    const scram = new SCRAM(resolveScramSaslConfig(sasl), host, port, logger, saslAuthenticate, DIGESTS.SHA512);
    return { authenticate: () => scram.authenticate() };
  };
}
