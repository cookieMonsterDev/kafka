/**
 * http://www.ietf.org/rfc/rfc5801.txt
 *
 * See org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerClientInitialResponse for
 * the official Java client implementation.
 *
 * The mechanism consists of a message from the client to the server. The client sends the "n,"
 * GS2 header, followed by the authorizationIdentity prefixed by "a=" (if present), followed by
 * ",", followed by a US-ASCII SOH character, followed by "auth=Bearer ", followed by the token
 * value, followed by a US-ASCII SOH character, followed by SASL extensions in OAuth "friendly"
 * format, then closed by two additional US-ASCII SOH characters.
 *
 * SASL extensions are optional and must be expressed as key-value pairs in an object. Each
 * expression is the extension entry key, followed by "=", followed by the extension entry value.
 * Each extension is separated by a US-ASCII SOH character. If extensions are not present, their
 * relative part in the message, including the US-ASCII SOH character, is omitted.
 *
 * The client may leave the authorization identity empty to indicate that it is the same as the
 * authentication identity.
 *
 * The server will verify the authentication token and verify that the authentication credentials
 * permit the client to login as the authorization identity. If both steps succeed, the user is
 * logged in.
 */
import { Decoder } from '../decoder';
import { Encoder } from '../encoder';

const SOH = String.fromCharCode(1); // Start Of Header

export interface OauthBearerSaslConfig {
  authorizationIdentity?: string | null;
}

export interface OauthBearerToken {
  value: string;
  extensions?: Record<string, string>;
}

function formatExtensions(extensions: Record<string, string> | undefined): string {
  if (extensions == null) return '';

  let msg = '';
  let prefix = '';
  for (const key in extensions) {
    msg += `${prefix}${key}=${extensions[key]}`;
    prefix = SOH;
  }

  return msg;
}

export async function oauthBearerRequest(
  { authorizationIdentity = null }: OauthBearerSaslConfig,
  oauthBearerToken: OauthBearerToken,
): Promise<{ encode(): Promise<Buffer> }> {
  const authzid = authorizationIdentity == null ? '' : `a=${authorizationIdentity}`;
  let ext = formatExtensions(oauthBearerToken.extensions);
  if (ext.length > 0) {
    ext = `${SOH}${ext}`;
  }

  const oauthMsg = `n,${authzid},${SOH}auth=Bearer ${oauthBearerToken.value}${ext}${SOH}${SOH}`;

  return {
    encode: async () => new Encoder().writeBytes(Buffer.from(oauthMsg)).buffer,
  };
}

/**
 * RFC 7628: an empty server payload means success. A JSON object with `status` other than
 * `ok` (typically `invalid_token`) is a failed exchange — Kafka may still return error_code 0
 * and then close the connection on the next API request.
 */
function parseOauthBearerStatus(payload: Buffer): { status: string } {
  const text = payload.toString('utf8').trim();
  if (!text || text[0] !== '{') return { status: 'ok' };

  try {
    const parsed = JSON.parse(text) as { status?: unknown };
    return { status: typeof parsed.status === 'string' ? parsed.status : 'ok' };
  } catch {
    return { status: 'ok' };
  }
}

export const oauthBearerResponse = {
  decode: async (rawData: Buffer): Promise<{ status: string }> => {
    if (rawData.length === 0) return { status: 'ok' };
    // Length-prefixed payloads start with an INT32, not `{`.
    if (rawData[0] === 0x7b) return parseOauthBearerStatus(rawData);

    try {
      const bytes = new Decoder(rawData).readBytes();
      if (bytes != null && bytes.length > 0) return parseOauthBearerStatus(bytes);
    } catch {
      return { status: 'ok' };
    }

    return { status: 'ok' };
  },
  parse: async (data: { status: string }): Promise<{ status: string }> => {
    if (data.status !== 'ok') {
      throw new Error(data.status);
    }
    return data;
  },
};
