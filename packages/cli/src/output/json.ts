const TOPIC_ID_KEY = 'topicId';
const TOPIC_ID_LENGTH = 16;

function bufferToUuid(buffer: Buffer): string {
  const hex = buffer.toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
}

/**
 * Recursively rewrites a value so `JSON.stringify` never throws and never silently loses
 * precision: a `bigint` becomes a decimal string (a JS `number` starts losing offsets past
 * 2^53, and `JSON.stringify` throws on a bare `bigint`), a `Buffer` becomes base64, and a
 * 16-byte `Buffer` under a `topicId` key (KIP-516) becomes its UUID string instead.
 */
export function toJsonSafe(value: unknown, keyHint?: string): unknown {
  if (typeof value === 'bigint') return value.toString();

  if (Buffer.isBuffer(value)) {
    if (keyHint === TOPIC_ID_KEY && value.length === TOPIC_ID_LENGTH) return bufferToUuid(value);
    return value.toString('base64');
  }

  if (Array.isArray(value)) return value.map((item) => toJsonSafe(item));

  if (value !== null && typeof value === 'object') {
    const result: Record<string, unknown> = {};
    for (const [key, nested] of Object.entries(value)) {
      result[key] = toJsonSafe(nested, key);
    }
    return result;
  }

  return value;
}

export function stringifyJsonSafe(value: unknown): string {
  return JSON.stringify(toJsonSafe(value));
}
