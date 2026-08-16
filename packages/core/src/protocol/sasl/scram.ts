/**
 * https://tools.ietf.org/html/rfc5802
 *
 * First, the client sends the "client-first-message" containing:
 *
 *  -> a GS2 header consisting of a flag indicating whether channel binding is
 *     supported-but-not-used, not supported, or used, and an optional SASL authorization identity;
 *
 *  -> SCRAM username and a random, unique nonce attributes.
 *
 * Note that the client's first message will always start with "n", "y", or "p"; otherwise, the
 * message is invalid and authentication MUST fail. This is important, as it allows for GS2
 * extensibility (e.g., to add support for security layers).
 */
import { Decoder } from '../decoder';
import { Encoder } from '../encoder';

const ENTRY_REGEX = /^([rsiev])=(.*)$/;

export interface ScramServerMessage {
  original: string;
  r?: string;
  s?: string;
  i?: string;
  e?: string;
  v?: string;
}

export function scramFirstMessageRequest({ clientFirstMessage }: { clientFirstMessage: string }): {
  encode(): Promise<Buffer>;
} {
  return { encode: async () => new Encoder().writeBytes(clientFirstMessage).buffer };
}

export function scramFinalMessageRequest({ finalMessage }: { finalMessage: string }): { encode(): Promise<Buffer> } {
  return { encode: async () => new Encoder().writeBytes(finalMessage).buffer };
}

/** Shared by both the first and final message exchange steps - the wire shape is identical. */
export const scramResponse = {
  decode: async (rawData: Buffer): Promise<Buffer> => new Decoder(rawData).readBytes() ?? Buffer.alloc(0),
  parse: async (data: Buffer): Promise<ScramServerMessage> => {
    const processed = Object.fromEntries(
      data
        .toString()
        .split(',')
        .map((entry) => {
          const match = ENTRY_REGEX.exec(entry);
          if (!match) throw new RangeError(`Malformed SCRAM server message entry: "${entry}"`);
          const [, key, value] = match;
          return [key as string, value];
        }),
    );

    return { original: data.toString(), ...processed };
  },
};
