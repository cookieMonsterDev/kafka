/**
 * Field names that hold credential material somewhere in `Admin`'s passthrough-only surface —
 * SCRAM and delegation-token results, mainly. Matched case-insensitively so it survives either
 * casing convention core happens to use for a given field.
 */
const SECRET_FIELD_NAMES: ReadonlySet<string> = new Set([
  'password',
  'saltedpassword',
  'salt',
  'hmac',
  'tokenhmac',
  'secretaccesskey',
  'sessiontoken',
  'passphrase',
]);

const REDACTED = '[REDACTED]';

/**
 * Recursively replaces any object property whose name matches a known secret field with a fixed
 * placeholder — applied to `admin call`'s result before it's ever written to stdout, unless the
 * caller explicitly opts out with `--show-secrets`.
 */
export function redactSecrets(value: unknown): unknown {
  if (Array.isArray(value)) return value.map((item) => redactSecrets(item));

  if (value !== null && typeof value === 'object' && !Buffer.isBuffer(value)) {
    const result: Record<string, unknown> = {};
    for (const [key, nested] of Object.entries(value)) {
      result[key] = SECRET_FIELD_NAMES.has(key.toLowerCase()) ? REDACTED : redactSecrets(nested);
    }
    return result;
  }

  return value;
}
