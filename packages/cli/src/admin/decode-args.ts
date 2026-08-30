import { CliUsageError } from '../args/coerce';

const BIGINT_PREFIX = 'bigint:';
const BASE64_PREFIX = 'base64:';
const UUID_PREFIX = 'uuid:';
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function decodeUuid(uuid: string): Buffer {
  if (!UUID_PATTERN.test(uuid)) {
    throw new CliUsageError(`invalid uuid: value "${uuid}"`);
  }
  return Buffer.from(uuid.replaceAll('-', ''), 'hex');
}

function decodeString(value: string): unknown {
  if (value.startsWith(BIGINT_PREFIX)) {
    const raw = value.slice(BIGINT_PREFIX.length);
    try {
      return BigInt(raw);
    } catch {
      throw new CliUsageError(`invalid bigint: value "${raw}"`);
    }
  }
  if (value.startsWith(BASE64_PREFIX)) {
    return Buffer.from(value.slice(BASE64_PREFIX.length), 'base64');
  }
  if (value.startsWith(UUID_PREFIX)) {
    return decodeUuid(value.slice(UUID_PREFIX.length));
  }
  return value;
}

/**
 * The mirror of `output/json.ts`'s encoder: a `bigint:`/`base64:`/`uuid:`-prefixed string decodes
 * back to a real `bigint`/`Buffer`, recursively through arrays and objects. Anything else passes
 * through untouched — most JSON values need no decoding at all.
 */
export function decodeArgs(value: unknown): unknown {
  if (typeof value === 'string') return decodeString(value);
  if (Array.isArray(value)) return value.map((item) => decodeArgs(item));
  if (value !== null && typeof value === 'object') {
    const result: Record<string, unknown> = {};
    for (const [key, nested] of Object.entries(value)) {
      result[key] = decodeArgs(nested);
    }
    return result;
  }
  return value;
}
