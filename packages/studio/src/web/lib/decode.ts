export type MessageDecoder = 'utf8' | 'json' | 'hex' | 'base64';

export interface MessageDecoderOption {
  readonly value: MessageDecoder;
  readonly label: string;
}

export const MESSAGE_DECODERS: readonly MessageDecoderOption[] = [
  { value: 'utf8', label: 'UTF-8' },
  { value: 'json', label: 'JSON' },
  { value: 'hex', label: 'Hex' },
  { value: 'base64', label: 'Base64' },
];

export interface DecodeResult {
  readonly text: string;
  /** Set when the chosen decoder couldn't make sense of the bytes — `text` still holds a fallback rendering. */
  readonly error?: string;
}

function base64ToBytes(base64: string): Uint8Array {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');
}

const utf8Decoder = new TextDecoder('utf-8', { fatal: false });

/** Renders one base64-encoded field (key, value, or header) as display text per the chosen decoder. `null` (Kafka's tombstone marker) always renders as the literal text `null`. */
export function decodeMessageField(base64: string | null, decoder: MessageDecoder): DecodeResult {
  if (base64 === null) return { text: 'null' };

  let bytes: Uint8Array;
  try {
    bytes = base64ToBytes(base64);
  } catch {
    return { text: base64, error: 'not valid base64' };
  }

  if (decoder === 'base64') return { text: base64 };
  if (decoder === 'hex') return { text: bytesToHex(bytes) };

  const text = utf8Decoder.decode(bytes);
  if (decoder === 'utf8') return { text };

  try {
    return { text: JSON.stringify(JSON.parse(text), null, 2) };
  } catch {
    return { text, error: 'not valid JSON' };
  }
}
