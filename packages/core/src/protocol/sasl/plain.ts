/**
 * http://www.ietf.org/rfc/rfc2595.txt
 *
 * The mechanism consists of a single message from the client to the server. The client sends the
 * authorization identity (identity to login as), followed by a US-ASCII NUL character, followed by
 * the authentication identity (identity whose password will be used), followed by a US-ASCII NUL
 * character, followed by the clear-text password. The client may leave the authorization identity
 * empty to indicate that it is the same as the authentication identity.
 *
 * The server will verify the authentication identity and password with the system authentication
 * database and verify that the authentication credentials permit the client to login as the
 * authorization identity. If both steps succeed, the user is logged in.
 */
import { Encoder } from '../encoder';

const NUL = String.fromCharCode(0);

export interface PlainSaslConfig {
  authorizationIdentity?: string | null;
  username: string;
  password: string;
}

export function plainRequest({ authorizationIdentity = null, username, password }: PlainSaslConfig): {
  encode(): Promise<Buffer>;
} {
  return {
    encode: async () => new Encoder().writeBytes([authorizationIdentity, username, password].join(NUL)).buffer,
  };
}

export const plainResponse = {
  decode: async (): Promise<boolean> => true,
  parse: async (): Promise<boolean> => true,
};
