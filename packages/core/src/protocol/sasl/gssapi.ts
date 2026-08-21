/**
 * Kafka SASL/GSSAPI (Kerberos) framing. Handshake mechanism name is `GSSAPI`.
 *
 * After SaslHandshake, the client and broker exchange GSS-API tokens inside
 * SaslAuthenticate `authBytes` until the context is established, then RFC 4752
 * wrap of the authorization identity. This module only length-prefixes those
 * opaque tokens; GSS itself comes from `sasl.gssProvider` or the optional
 * `kerberos` peer.
 *
 * @see https://kafka.apache.org/43/security/authentication-using-sasl/
 * @see https://www.rfc-editor.org/rfc/rfc4752
 */
import { Decoder } from '../decoder';
import { Encoder } from '../encoder';

/** Java `sasl.kerberos.service.name` default. */
export const DEFAULT_GSSAPI_SERVICE_NAME = 'kafka';

/** Safety cap on SaslAuthenticate rounds (GSS init + RFC 4752 wrap). */
export const MAX_GSSAPI_ROUNDS = 16;

/** One GSS/RFC 4752 round presented to {@link GssTokenProvider}. */
export interface GssTokenChallenge {
  /** Broker token from the previous round, or `null` on the first call. */
  serverToken: Buffer | null;
  host: string;
  port: number;
  serviceName: string;
  principal?: string;
}

/** Next client token, and whether this is the last client message. */
export interface GssTokenStep {
  token: Buffer;
  /** When true, this token is the last the client will send (it is still transmitted if non-empty). */
  complete: boolean;
}

/**
 * Stateful GSS token factory. Called once per SASL round until `complete`.
 * The implementation owns the GSS context (library binding or pre-built tokens).
 */
export type GssTokenProvider = (challenge: GssTokenChallenge) => Promise<GssTokenStep>;

export interface GssapiSaslConfig {
  /** Kerberos service name used in the SPN (`serviceName@host`). Default `kafka`. */
  serviceName?: string;
  /** Client principal (`user@REALM`), passed to the optional `kerberos` binding. */
  principal?: string;
  /** Path to a client keytab. Applied as `KRB5_CLIENT_KTNAME` / `KRB5_KTNAME` when using `kerberos`. */
  keytab?: string;
  /** Path to `krb5.conf`. Applied as `KRB5_CONFIG` when using `kerberos`. */
  krb5?: string;
  /** RFC 4752 authorization identity; defaults to the client principal when wrapping. */
  authorizationIdentity?: string;
  /** User-supplied GSS stepper. When omitted, the optional `kerberos` package is loaded. */
  gssProvider?: GssTokenProvider;
}

export function gssapiRequest(token: Buffer): { encode(): Promise<Buffer> } {
  return {
    encode: async () => new Encoder().writeBytes(token).buffer,
  };
}

export const gssapiResponse = {
  decode: async (rawData: Buffer): Promise<Buffer> => new Decoder(rawData).readBytes() ?? Buffer.alloc(0),
  parse: async (data: Buffer): Promise<Buffer> => data,
};
