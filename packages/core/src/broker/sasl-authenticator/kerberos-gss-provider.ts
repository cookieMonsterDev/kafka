import { createRequire } from 'node:module';
import { KafkaSASLAuthenticationError } from '../../errors';
import {
  DEFAULT_GSSAPI_SERVICE_NAME,
  type GssTokenProvider,
  type GssTokenStep,
  type GssapiSaslConfig,
} from '../../protocol/sasl/gssapi';

const requireKerberos = createRequire(import.meta.url);

/** Subset of `kerberos` used for Kafka SASL/GSSAPI. */
export interface KerberosClientLike {
  contextComplete: boolean;
  username: string | null;
  step(challenge: string): Promise<string | null>;
  wrap(challenge: string, options?: { user?: string; protect?: boolean }): Promise<string | null>;
  unwrap(challenge: string): Promise<string | null>;
}

export interface KerberosModuleLike {
  initializeClient(
    service: string,
    options?: { principal?: string; flags?: number; mechOID?: number },
  ): Promise<KerberosClientLike>;
  GSS_MECH_OID_KRB5?: number;
  GSS_C_MUTUAL_FLAG?: number;
  GSS_C_SEQUENCE_FLAG?: number;
  GSS_C_INTEG_FLAG?: number;
}

export type LoadKerberos = () => Promise<KerberosModuleLike>;

export async function loadKerberosModule(): Promise<KerberosModuleLike> {
  try {
    return requireKerberos('kerberos') as KerberosModuleLike;
  } catch (cause) {
    throw new KafkaSASLAuthenticationError(
      'SASL GSSAPI: provide sasl.gssProvider or install the optional `kerberos` package',
      { cause },
    );
  }
}

function decodeKerberosToken(value: string | null | undefined): Buffer {
  if (value == null || value.length === 0) return Buffer.alloc(0);
  return Buffer.from(value, 'base64');
}

/**
 * Points MIT Kerberos at a keytab / krb5.conf for the duration of `fn`.
 * These variables are process-wide; concurrent GSSAPI clients with different
 * keytabs can race.
 */
export function applyKerberosEnv(keytab?: string, krb5?: string): () => void {
  const previous = {
    KRB5_CLIENT_KTNAME: process.env.KRB5_CLIENT_KTNAME,
    KRB5_KTNAME: process.env.KRB5_KTNAME,
    KRB5_CONFIG: process.env.KRB5_CONFIG,
  };

  if (keytab) {
    process.env.KRB5_CLIENT_KTNAME = keytab;
    process.env.KRB5_KTNAME = keytab;
  }
  if (krb5) process.env.KRB5_CONFIG = krb5;

  return () => {
    restoreEnv('KRB5_CLIENT_KTNAME', previous.KRB5_CLIENT_KTNAME);
    restoreEnv('KRB5_KTNAME', previous.KRB5_KTNAME);
    restoreEnv('KRB5_CONFIG', previous.KRB5_CONFIG);
  };
}

function restoreEnv(name: 'KRB5_CLIENT_KTNAME' | 'KRB5_KTNAME' | 'KRB5_CONFIG', value: string | undefined): void {
  if (value === undefined) delete process.env[name];
  else process.env[name] = value;
}

/**
 * Default GSS stepper: `kerberos.initializeClient` + RFC 4752 wrap via `unwrap`/`wrap`.
 */
export function createKerberosGssProvider(
  sasl: GssapiSaslConfig,
  loadKerberos: LoadKerberos = loadKerberosModule,
): GssTokenProvider {
  let clientPromise: Promise<KerberosClientLike> | undefined;
  let phase: 'gss' | 'wrap' = 'gss';

  const getClient = async (host: string): Promise<KerberosClientLike> => {
    clientPromise ??= (async () => {
      const kerberos = await loadKerberos();
      const serviceName = sasl.serviceName ?? DEFAULT_GSSAPI_SERVICE_NAME;
      const flags =
        (kerberos.GSS_C_MUTUAL_FLAG ?? 2) | (kerberos.GSS_C_SEQUENCE_FLAG ?? 8) | (kerberos.GSS_C_INTEG_FLAG ?? 32);
      return kerberos.initializeClient(`${serviceName}@${host}`, {
        principal: sasl.principal,
        flags,
        mechOID: kerberos.GSS_MECH_OID_KRB5,
      });
    })();
    return clientPromise;
  };

  return async (challenge): Promise<GssTokenStep> => {
    const restore = applyKerberosEnv(sasl.keytab, sasl.krb5);
    try {
      const client = await getClient(challenge.host);

      if (phase === 'gss') {
        const input =
          challenge.serverToken && challenge.serverToken.length > 0 ? challenge.serverToken.toString('base64') : '';
        const output = await client.step(input);
        const token = decodeKerberosToken(output);

        if (client.contextComplete) {
          phase = 'wrap';
          return { token, complete: false };
        }

        return { token, complete: false };
      }

      if (!challenge.serverToken || challenge.serverToken.length === 0) {
        throw new KafkaSASLAuthenticationError(
          'SASL GSSAPI: broker did not send an RFC 4752 wrap token after GSS context establishment',
        );
      }

      const unwrapped = await client.unwrap(challenge.serverToken.toString('base64'));
      const authzid = sasl.authorizationIdentity ?? sasl.principal ?? client.username ?? '';
      const wrapped = await client.wrap(unwrapped ?? '', { user: authzid });
      return { token: decodeKerberosToken(wrapped), complete: true };
    } finally {
      restore();
    }
  };
}
